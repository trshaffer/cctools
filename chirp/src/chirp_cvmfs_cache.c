/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <assert.h>
#include <stdbool.h>
#include <errno.h>
#include <stdio.h>
#include <time.h>
#include <unistd.h>
#include <limits.h>

#include <cache_plugin/libcvmfs_cache.h>

#include "chirp_reli.h"

struct pretty_hash {
	char str[2 * sizeof(struct cvmcache_hash) + 2];
};

static void pretty_print(const struct cvmcache_hash *id, struct pretty_hash *out) {
	assert(id);
	assert(out);
	sprintf(out->str, "%X.", id->algorithm);
	for (size_t i = 0; i < sizeof(id->digest); i++) {
		sprintf(&out->str[i + 3], "%x", id->digest[i]);
	}
}

static time_t stoptime() {
	return time(NULL) + 5;
}

static const char *host() {
	return "localhost";
}

static int chirp_chrefcnt(struct cvmcache_hash *id, int32_t change_by) {
	char path[PATH_MAX];
	struct pretty_hash hash;
	print_hash(id, &hash);
	snprintf(path, PATH_MAX, "/%s/refcount", hash->str);
	struct chirp_file *c = chirp_reli_open(host(), path, 0, 0, stoptime());
	if (chirp_reli_close(c, stoptime()) < 0) {
		fprintf(stderr, "unable to close Chirp file [something]: %s\n", strerror(errno));
		return CVMCACHE_STATUS_IOERR;
	}
	return CVMCACHE_STATUS_OK;
}

static int chirp_obj_info(struct cvmcache_hash *id, struct cvmcache_object_info *info) {
	return CVMCACHE_STATUS_OK;
}

static int chirp_pread(struct cvmcache_hash *id, uint64_t offset, uint32_t *size, unsigned char *buffer) {
	return CVMCACHE_STATUS_OK;
}

static int chirp_start_txn(struct cvmcache_hash *id, uint64_t txn_id, struct cvmcache_object_info *info) {
	return CVMCACHE_STATUS_OK;
}

static int chirp_write_txn(uint64_t txn_id, unsigned char *buffer, uint32_t size) {
	return CVMCACHE_STATUS_OK;
}

static int chirp_commit_txn(uint64_t txn_id) {
	return CVMCACHE_STATUS_OK;
}

static int chirp_abort_txn(uint64_t txn_id) {
	return CVMCACHE_STATUS_OK;
}

static int chirp_info(struct cvmcache_info *info) {
	return CVMCACHE_STATUS_OK;
}

static int chirp_shrink(uint64_t shrink_to, uint64_t *used) {
	return CVMCACHE_STATUS_OK;
}

static int chirp_listing_begin(uint64_t lst_id, enum cvmcache_object_type type) {
	return CVMCACHE_STATUS_OK;
}

static int chirp_listing_next(int64_t listing_id, struct cvmcache_object_info *item) {
	return CVMCACHE_STATUS_OK;
}

static int chirp_listing_end(int64_t listing_id) {
	return CVMCACHE_STATUS_OK;
}

int main(int argc, char *argv[]) {
	struct cvmcache_context *ctx;

	if (argc < 2) {
		printf("usage: %s <config-file>\n", argv[0]);
		return 1;
	}

	cvmcache_init_global();

	cvmcache_option_map *options = cvmcache_options_init();
	if (cvmcache_options_parse(options, argv[1]) != 0) {
		printf("cannot parse options file %s\n", argv[1]);
		return 1;
	}
	char *locator = cvmcache_options_get(options, "CVMFS_CACHE_EXTERNAL_LOCATOR");
	if (locator == NULL) {
		printf("CVMFS_CACHE_EXTERNAL_LOCATOR missing\n");
		cvmcache_options_fini(options);
		return 1;
	}

	cvmcache_spawn_watchdog(NULL);

	struct cvmcache_callbacks callbacks;
	memset(&callbacks, 0, sizeof(callbacks));
	callbacks.cvmcache_chrefcnt = chirp_chrefcnt;
	callbacks.cvmcache_obj_info = chirp_obj_info;
	callbacks.cvmcache_pread = chirp_pread;
	callbacks.cvmcache_start_txn = chirp_start_txn;
	callbacks.cvmcache_write_txn = chirp_write_txn;
	callbacks.cvmcache_commit_txn = chirp_commit_txn;
	callbacks.cvmcache_abort_txn = chirp_abort_txn;
	callbacks.cvmcache_info = chirp_info;
	callbacks.cvmcache_shrink = chirp_shrink;
	callbacks.cvmcache_listing_begin = chirp_listing_begin;
	callbacks.cvmcache_listing_next = chirp_listing_next;
	callbacks.cvmcache_listing_end = chirp_listing_end;
	callbacks.capabilities = CVMCACHE_CAP_ALL;

	ctx = cvmcache_init(&callbacks);
	int retval = cvmcache_listen(ctx, locator);
	if (!retval) {
		fprintf(stderr, "failed to listen on %s\n", locator);
		return 1;
	}
	printf("Listening for cvmfs clients on %s\n", locator);
	printf("NOTE: this process needs to run as user cvmfs\n\n");

	// Starts the I/O processing thread
	cvmcache_process_requests(ctx, 0);

	if (!cvmcache_is_supervised()) {
		printf("Press <R ENTER> to ask clients to release nested catalogs\n");
		printf("Press <Ctrl+D> to quit\n");
		while (true) {
			char buf;
			int retval = read(fileno(stdin), &buf, 1);
			if (retval != 1)
				break;
			if (buf == 'R') {
				printf("  ... asking clients to release nested catalogs\n");
				cvmcache_ask_detach(ctx);
			}
		}
		cvmcache_terminate(ctx);
	}

	cvmcache_wait_for(ctx);
	printf("  ... good bye\n");

	cvmcache_options_free(locator);
	cvmcache_options_fini(options);
	cvmcache_terminate_watchdog();
	cvmcache_cleanup_global();
	return 0;
}
