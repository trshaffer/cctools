#ifndef DATASWARM_TEST_H
#define DATASWARM_TEST_H

#include "ds_manager.h"
#include "ds_worker_rep.h"

int wait_for_rpcs(struct ds_manager *m, struct ds_worker_rep *r);
void dataswarm_test_script( struct ds_manager *m, struct ds_worker_rep *r, int phase);

#endif

