
#include "ds_manager.h"
#include "ds_worker_rep.h"
#include "ds_blob_rep.h"
#include "ds_task_rep.h"
#include "ds_rpc.h"

#include "debug.h"
#include "stringtools.h"
#include "hash_table.h"
#include "itable.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int wait_for_rpcs(struct ds_manager *m, struct ds_worker_rep *r) {
	uint64_t key;

	ds_rpc_get_response(m,r);

	/* check blob responses */
	struct ds_blob_rep *b;
	itable_firstkey(r->blob_of_rpc);
	while((itable_nextkey(r->blob_of_rpc, &key, (void **) &b))) {
		if(b->result == DS_RESULT_PENDING) {
			return 0;
		} else if (b->result != DS_RESULT_SUCCESS) {
			debug(D_DATASWARM, "rpc for blob %s failed with: %d", b->blobid, b->result);
			return -1;
		}
	}

	struct ds_task_rep *t;
	itable_firstkey(r->task_of_rpc);
	while((itable_nextkey(r->task_of_rpc, &key, (void **) &t))) {
		if(t->result == DS_RESULT_PENDING) {
			return 0;
		} else if (b->result != DS_RESULT_SUCCESS) {
			debug(D_DATASWARM, "rpc for task %s failed with: %d", t->taskid, t->result);
			return -1;
		}
	}

	/* check that all tasks are done */
	char *taskid;
	hash_table_firstkey(r->tasks);
	while((hash_table_nextkey(r->tasks, &taskid, (void **) &t))) {
		if(t->state == DS_TASK_WORKER_STATE_SUBMITTED) {
			/* task has not reached completed state after submission */
			return 0;
		}
	}

	return 1;
}

void dataswarm_test_script( struct ds_manager *m, struct ds_worker_rep *r, int phase )
{
	const char *bloba = "abc123";
	const char *blobb = "xyz456";
	char *taskid = NULL;
	/* Create a simple task that reads from bloba mounted as myinput and writes to blob mounted as stdout. */
	struct jx *taskinfo = jx_objectv("command",   jx_string("wc -l myinput"),
			"namespace", jx_objectv(bloba, jx_objectv("type", jx_string("path"),
					"path", jx_string("myinput"),
					"mode", jx_string("R"),
					NULL),
				blobb, jx_objectv("type", jx_string("stdout"),
					NULL),
				NULL),
			NULL);

	switch (phase) {
		case 0:
			ds_manager_add_blob_to_worker(m, r, bloba),
			ds_manager_add_blob_to_worker(m, r, blobb),

			ds_rpc_blob_delete(m,r,bloba);
			ds_rpc_blob_delete(m,r,blobb);
			break;
		case 1:
			ds_rpc_blob_create(m,r,bloba,100000,NULL);
			ds_rpc_blob_create(m,r,blobb,100000,NULL);
			break;
		case 2:
			ds_rpc_blob_put(m,r,bloba,"/usr/share/dict/words");
			break;
		case 3:
			ds_rpc_blob_commit(m,r,bloba);
			break;
		case 4:
			/* submit task to manager */
			taskid = ds_manager_submit_task(m, taskinfo);

			/* declare task in worker */
			ds_manager_add_task_to_worker(m,r,taskid);

			/* send task to worker */
			ds_rpc_task_submit(m,r,taskid);
			break;
		case 5:
			ds_rpc_blob_get(m,r,blobb,"/dev/stdout");
			break;
		case 6:
			ds_rpc_task_remove(m,r,taskid);
			free(taskid);

			ds_rpc_blob_delete(m,r,bloba);
			//ds_rpc_blob_delete(m,r,blobb);
			break;
		case 7:
			debug(D_DATASWARM, "Done testing this worker.");
			exit(0);
	}
}

void dataswarm_test_script_old_sync( struct ds_manager *m, struct ds_worker_rep *r )
{
	const char *bloba = "abc123";
	const char *blobb = "xyz456";

	ds_rpc_blob_delete(m,r,bloba);
	ds_rpc_blob_delete(m,r,blobb);

	sleep(1);

	ds_rpc_blob_create(m,r,bloba,100000,NULL);
	ds_rpc_blob_put(m,r,bloba,"/usr/share/dict/words");
	ds_rpc_blob_commit(m,r,bloba);


	ds_rpc_blob_create(m,r,blobb,100000,NULL);

	sleep(1);

	/* Create a simple task that reads from bloba mounted as myinput and writes to blob mounted as stdout. */
	char *taskinfo = string_format("{ \"task-id\": \"%s\",\"command\" : \"wc -l myinput\", \"namespace\" : { \"%s\" : {\"type\" : \"path\", \"path\" : \"myinput\", \"mode\" : \"R\" }, \"%s\" : {\"type\" : \"stdout\" } } }","t93",bloba,blobb);
	ds_rpc_task_submit(m,r,taskinfo);
	free(taskinfo);

	sleep(5);

	// need to wait for task to complete
	ds_rpc_blob_get(m,r,blobb,"/dev/stdout");

	sleep(1);

	ds_rpc_task_remove(m,r,"t93");

	sleep(1);


	ds_rpc_blob_delete(m,r,bloba);
	ds_rpc_blob_delete(m,r,blobb);

}

/* vim: set noexpandtab tabstop=4: */
