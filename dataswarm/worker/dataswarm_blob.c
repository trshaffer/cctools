
#include "dataswarm_blob.h"
#include "dataswarm_message.h"

#include "stringtools.h"
#include "debug.h"
#include "jx_print.h"
#include "jx_parse.h"
#include "jx.h"
#include "delete_dir.h"
#include "create_dir.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include <errno.h>
#include <string.h>

struct jx *dataswarm_blob_create( struct jx *params )
{
    const char *blobid =  jx_lookup_string(params, "blob-id");
    if(!blobid) {
        return dataswarm_message_error_response(DS_MSG_MALFORMED_PARAMETERS, params);
    }


    jx_int_t size = jx_lookup_integer(params, "size");
    if(!size) {
        return dataswarm_message_error_response(DS_MSG_MALFORMED_PARAMETERS, params);
    }

    // XXX should here check for available space

    struct jx *meta =  jx_lookup(params, "metadata");
    struct jx *user =  jx_lookup(params, "userdata");

	char *blob_dir = string_format("rw/%s",blobid);
	char *blob_meta = string_format("rw/%s/meta",blob_dir);
	char *blob_user = string_format("rw/%s/user",blob_dir);

	if(!mkdir(blob_dir,0777)) {
		debug(D_DATASWARM,"couldn't mkdir %s: %s",blob_dir,strerror(errno));
        return dataswarm_message_state_response("internal-failure", "could not create blob directory");
    }

    if(meta) {
		FILE *file = fopen(blob_meta,"w");
		if(!file) {
			debug(D_DATASWARM,"couldn't open %s: %s",blob_meta,strerror(errno));
            return dataswarm_message_state_response("internal-failure", "could not write metadata");
        }
        jx_print_stream(meta,file);
        fclose(file);
    }

    if(user) {
		FILE *file = fopen(blob_user,"w");
		if(!file) {
			debug(D_DATASWARM,"couldn't open %s: %s",blob_user,strerror(errno));
            return dataswarm_message_state_response("internal-failure", "could not write userdata");
        }
        jx_print_stream(user,file);
        fclose(file);
    }

	free(blob_dir);
	free(blob_meta);

	return dataswarm_message_state_response("allocated", NULL);
}

struct jx *dataswarm_blob_put( struct link *l, struct jx *params )
{
    const char *blobid =  jx_lookup_string(params, "blob-id");
    if(!blobid) {
        return dataswarm_message_error_response(DS_MSG_MALFORMED_PARAMETERS, params);
    }

	char *blob_dir = string_format("rw/%s",blobid);
	char *blob_data = string_format("rw/%s/data",blob_dir);

	char line[32];

	// XXX should set timeout more appropriately
	time_t stoptime = time(0) + 3600;

    if(!link_readline(l,line,sizeof(line),stoptime)) {
        debug(D_DATASWARM,"couldn't read file length: %s: %s",blob_dir,strerror(errno));
        return dataswarm_message_state_response("internal-failure", "could not read file length");
    }

    int64_t length = atoll(line);
    // XXX should here check for available space
    // return dataswarm_message_state_response("internal-failure", "no space available");
    //
    // XXX should handle directory transfers.

    FILE *file = fopen(blob_data,"w");
    free(blob_dir);
    free(blob_data);

    if(!file) {
        debug(D_DATASWARM,"couldn't open %s: %s",blob_data,strerror(errno));
        return dataswarm_message_state_response("internal-failure", "could not open file for writing");
    }

    int bytes_transfered = link_stream_to_file(l,file,length,stoptime);
    fclose(file);

    if(bytes_transfered != length) {
        debug(D_DATASWARM,"couldn't stream to %s: %s",blob_data,strerror(errno));
        return dataswarm_message_state_response("internal-failure", "could not stream file");
    }

	return dataswarm_message_state_response("written", NULL);
}

struct jx *dataswarm_blob_get( struct link *l, struct jx *params )
{
    const char *blobid =  jx_lookup_string(params, "blob-id");
    if(!blobid) {
        return dataswarm_message_error_response(DS_MSG_MALFORMED_PARAMETERS, params);
    }

	char *blob_dir = string_format("rw/%s",blobid);
	char *blob_data = string_format("rw/%s/data",blob_dir);

	struct stat info;
	int status = stat(blob_data,&info);

    free(blob_dir);
    free(blob_data);

	if(!status) {
        debug(D_DATASWARM,"couldn't stat blob: %s: %s",blob_data,strerror(errno));
        return dataswarm_message_state_response("internal-failure", "could not stat file");
    }

    FILE *file = fopen(blob_data,"r");
    free(blob_dir);
    free(blob_data);

    if(!file) {
        debug(D_DATASWARM,"couldn't open %s: %s",blob_data,strerror(errno));
        return dataswarm_message_state_response("internal-failure", "could not open file for reading");
    }

    int64_t length = info.st_size;
    char *line = string_format("%lld\n",(long long)length);

    // XXX should set timeout more appropriately
    time_t stoptime = time(0) + 3600;
    link_write(l,line,strlen(line),stoptime);
    free(line);

    // XXX should handle directory transfers.

    int bytes_transfered = link_stream_from_file(l,file,length,stoptime);
    fclose(file);

    if(bytes_transfered != length) {
        debug(D_DATASWARM,"couldn't stream from %s: %s",blob_data,strerror(errno));
        return dataswarm_message_state_response("internal-failure", "could not stream file");
    }

	return dataswarm_message_state_response("written", NULL);
}


/*
dataswarm_blob_commit converts a read-write blob into
a read-only blob, fixing its size and properties for all time,
allowing the object to be duplicated to other nodes.
*/

struct jx *dataswarm_blob_commit( struct jx *params )
{
    const char *blobid =  jx_lookup_string(params, "blob-id");
    if(!blobid) {
        return dataswarm_message_error_response(DS_MSG_MALFORMED_PARAMETERS, params);
    }

	char *ro_name = string_format("ro/%s",blobid);
	char *rw_name = string_format("rw/%s",blobid);

    int status = rename(ro_name,rw_name);
    free(ro_name);
    free(rw_name);

    if(status == 0) {
        return dataswarm_message_state_response("committed", NULL);
    } else {
		debug(D_DATASWARM,"couldn't commit %s: %s",blobid,strerror(errno));
        return dataswarm_message_state_response("internal-error", "could not commit blob");
    }
}

/*
dataswarm_blob_delete moves the blob to the deleting
dir, and then also deletes the object synchronously.  This ensures
that the delete (logically) occurs atomically, so that if the delete
fails or the worker crashes, all deleted blobs can be cleaned up on restart.
*/


struct jx *dataswarm_blob_delete( struct jx *params )
{
    const char *blobid =  jx_lookup_string(params, "blob-id");
    if(!blobid) {
        return dataswarm_message_error_response(DS_MSG_MALFORMED_PARAMETERS, params);
    }

	char *ro_name = string_format("ro/%s",blobid);
	char *rw_name = string_format("rw/%s",blobid);
	char *deleting_name = string_format("deleting/%s",blobid);

    int status = (rename(ro_name,deleting_name)==0 || rename(rw_name,deleting_name)==0);
	free(ro_name);
	free(rw_name);
	free(deleting_name);

    if(!status) {
		debug(D_DATASWARM,"couldn't delete %s: %s",blobid,strerror(errno));
        return dataswarm_message_state_response("internal-error", "could not delete blob");
    }

    delete_dir(deleting_name);

	return dataswarm_message_state_response("deleting", NULL);
}


/*
dataswarm_blob_copy message requests a blob to be duplicated. The new copy is
read-write with a new blob-id.
*/

struct jx *dataswarm_blob_copy( struct jx *params )
{
    const char *blobid_src =  jx_lookup_string(params, "blob-id-source");
    if(!blobid_src) {
        return dataswarm_message_error_response(DS_MSG_MALFORMED_PARAMETERS, params);
    }

    const char *blobid =  jx_lookup_string(params, "blob-id");
    if(!blobid) {
        return dataswarm_message_error_response(DS_MSG_MALFORMED_PARAMETERS, params);
    }

    /* XXX do the copying */

	return dataswarm_message_state_response("allocated", NULL);
}