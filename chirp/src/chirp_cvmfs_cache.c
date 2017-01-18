/*
Copyright (C) 2017- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <assert.h>
#include <inttypes.h>
#include <stdbool.h>
#include <errno.h>
#include <stdio.h>
#include <time.h>
#include <unistd.h>
#include <limits.h>

#include <uuid/uuid.h>
#include <cache_plugin/libcvmfs_cache.h>

#include "chirp_reli.h"

#define GETBUFFER(path, buf) do { \
	if (chirp_reli_getfile_buffer(chirp_host, path, buf, stoptime()) < 0) { \
		fprintf(stderr, "chirp_cvmfs_cache: failed to get %s on %s: %s\n", \
			path, chirp_host, strerror(errno)); \
		return CVMCACHE_STATUS_IOERR; \
	} \
} while (0)

#define GETBUFFER_OPT(path, buf) do { \
	if (chirp_reli_getfile_buffer(chirp_host, path, buf, stoptime()) < 0) { \
		if (errno == ENOENT) { \
			*(buf) = NULL; \
		} else { \
			fprintf(stderr, "chirp_cvmfs_cache: failed to get %s on %s: %s\n", \
				path, chirp_host, strerror(errno)); \
			return CVMCACHE_STATUS_IOERR; \
		} \
	} \
} while (0)

#define PUTBUFFER(path, buf) do { \
	if (chirp_reli_putfile_buffer(chirp_host, path, buf, 0644, strlen(buf), stoptime()) < 0) { \
		fprintf(stderr, "chirp_cvmfs_cache: failed to write %s on %s: %s\n", \
			path, chirp_host, strerror(errno)); \
		return CVMCACHE_STATUS_IOERR; \
	} \
} while (0)

#define CHECK_EXISTS(path) do { \
	struct chirp_stat cs; \
	if (chirp_reli_stat(chirp_host, path, &cs, stoptime()) < 0) { \
		return errno == ENOENT ? CVMCACHE_STATUS_NOENTRY : CVMCACHE_STATUS_IOERR; \
	} \
} while (0)

#define MAKE_DIR(path) do { \
	if (chirp_reli_mkdir(chirp_host, path, 0755, stoptime()) < 0) { \
		fprintf(stderr, "chirp_cvmfs_cache: failed to make new directory %s on %s: %s\n", \
			path, chirp_host, strerror(errno)); \
		return CVMCACHE_STATUS_IOERR; \
	} \
} while (0)

#define ENSURE_DIR(path) do { \
	if (chirp_reli_mkdir(chirp_host, path, 0755, stoptime()) < 0 && errno != EEXIST) { \
		fprintf(stderr, "chirp_cvmfs_cache: failed to make directory %s on %s: %s\n", \
			path, chirp_host, strerror(errno)); \
		return CVMCACHE_STATUS_IOERR; \
	} \
} while (0)

#define OPEN_FILE(f, path, flags, mode) do { \
	*f = chirp_reli_open(chirp_host, path, flags, mode, stoptime()); \
	if (!*f) { \
		fprintf(stderr, "chirp_cvmfs_cache: failed to open %s on %s: %s\n", \
			path, chirp_host, strerror(errno)); \
		return CVMCACHE_STATUS_IOERR; \
	} \
} while (0)

#define FSTAT_FILE(f, cs) do { \
	if (chirp_reli_fstat(f, cs, stoptime()) < 0) { \
		fprintf(stderr, "chirp_cvmfs_cache: failed fstat %s on %s: %s\n", \
			chirp_host, strerror(errno)); \
		return CVMCACHE_STATUS_IOERR; \
	} \
} while ()

#define PREAD_FILE(f, buffer, nbytes, offset) do { \
	*nbytes = chirp_reli_pread(f, buffer, *nbytes, offset, stoptime()); \
	if (*nbytes < 0) { \
		fprintf(stderr, "chirp_cvmfs_host: failed to pread %s on %s: %s\n", \
			path, chirp_host, strerror(errno)); \
		return CVMCACHE_STATUS_IOERR; \
	} \
} while (0)

#define CLOSE_FILE(f) do { \
	if (chirp_reli_close(f, stoptime()) < 0) { \
		fprintf(stderr, "chirp_cvmfs_cache: failed close on %s: %s\n", \
			chirp_host, strerror(errno)); \
			return CVMCACHE_STATUS_IOERR; \
	} \
} while (0)

char chirp_host[4096] = "localhost";
char session_id[64];


struct pretty_hash {
	char str[2 * sizeof(struct cvmcache_hash) + 2];
};


static void hash_prettify(const struct cvmcache_hash *id, struct pretty_hash *out) {
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


static int chirp_chrefcnt(struct cvmcache_hash *id, int32_t change_by) {
	assert(id);

	struct pretty_hash hash;
	hash_prettify(id, &hash);

	char path[PATH_MAX];
	snprintf(path, sizeof(path), "/%s/", hash.str);
	CHECK_EXISTS(path);

	int rc;
	char *s;
	snprintf(path, sizeof(path), "/%s/refcount/%s", hash.str, session_id);
	GETBUFFER_OPT(path, &s);
	if (s) {
		rc = atoi(s);
		free(s);
	} else {
		rc = 0;
	}

	rc += change_by;
	if (rc < 0) {
		return CVMCACHE_STATUS_BADCOUNT;
	}

	char buf[64];
	snprintf(buf, sizeof(buf), "%d", rc);
	PUTBUFFER(path, buf);

	return CVMCACHE_STATUS_OK;
}


static int chirp_obj_info(struct cvmcache_hash *id, struct cvmcache_object_info *info) {
	assert(id);
	assert(info);

	struct pretty_hash hash;
	hash_prettify(id, &hash);

	char path[PATH_MAX];
	snprintf(path, sizeof(path), "/%s/", hash.str);
	CHECK_EXISTS(path);

	struct cvmcache_object_info out;
	memcpy(&out.id, id, sizeof(out.id));

	char *s;
	snprintf(path, sizeof(path), "/%s/size", hash.str);
	GETBUFFER(path, &s);
	out.size = atoll(s);
	free(s);

	snprintf(path, sizeof(path), "/%s/type", hash.str);
	GETBUFFER(path, &s);
	switch (s[0]) {
		case 'r':
		out.type = CVMCACHE_OBJECT_REGULAR;
		break;
		case 'c':
		out.type = CVMCACHE_OBJECT_CATALOG;
		break;
		case 'v':
		out.type = CVMCACHE_OBJECT_VOLATILE;
		break;
		default:
		fprintf(stderr, "chirp_cvmfs_cache: invalid type %s for %s\n",
			s, hash.str);
		free(s);
		return CVMCACHE_STATUS_IOERR;
	}
	free(s);

	snprintf(path, sizeof(path), "/%s/pinned", hash.str);
	GETBUFFER(path, &s);
	out.pinned = atoi(s);
	free(s);

	snprintf(path, sizeof(path), "/%s/description", hash.str);
	GETBUFFER_OPT(path, &s);
	out.description = s;

	memcpy(info, &out, sizeof(*info));
	return CVMCACHE_STATUS_OK;
}


static int chirp_pread(struct cvmcache_hash *id, uint64_t offset, uint32_t *size, unsigned char *buffer) {
	assert(id);
	assert(size);
	assert(buffer);

	struct pretty_hash hash;
	hash_prettify(id, &hash);

	char path[PATH_MAX];
	snprintf(path, sizeof(path), "/%s/", hash.str);
	CHECK_EXISTS(path);

	snprintf(path, sizeof(path), "/%s/data", hash.str);
	struct chirp_file *f;
	OPEN_FILE(&f, path, O_RDONLY, 0);

	struct chirp_stat cs;
	FSTAT_FILE(f, &cs);
	if ((int) offset > cs.cst_size) {
		return CVMCACHE_STATUS_OUTOFBOUNDS;
	}

	int nbytes = *size < cs.cst_size - offset ? *size : cs.cst_size - offset;
	PREAD_FILE(f, buffer, &nbytes, offset);
	*size = nbytes;

	CLOSE_FILE(f);

	return CVMCACHE_STATUS_OK;
}


static int chirp_start_txn(struct cvmcache_hash *id, uint64_t txn_id, struct cvmcache_object_info *info) {
	assert(id);
	assert(info);

	struct pretty_hash hash;
	hash_prettify(id, &hash);

	char path[PATH_MAX];
	snprintf(path, sizeof(path), "/txn/%s.%" PRIu64, session_id, txn_id);
	MAKE_DIR(path);

	snprintf(path, sizeof(path), "/txn/%s.%" PRIu64 "/id", session_id, txn_id);
	PUTBUFFER(path, hash.str);

	const char *s;
	snprintf(path, sizeof(path), "/txn/%s.%" PRIu64 "/type", session_id, txn_id);
	switch (info->type) {
		case CVMCACHE_OBJECT_REGULAR:
		s = "r";
		break;
		case CVMCACHE_OBJECT_CATALOG:
		s = "c";
		break;
		case CVMCACHE_OBJECT_VOLATILE:
		s = "v";
		break;
		default:
		return CVMCACHE_STATUS_MALFORMED;
	}
	PUTBUFFER(path, s);

	snprintf(path, sizeof(path), "/txn/%s.%" PRIu64 "/refcount", session_id, txn_id);
	MAKE_DIR(path);

	s = "1";
	snprintf(path, sizeof(path), "/%s/refcount/%s", hash.str, session_id);
	PUTBUFFER(path, s);

	if (info->description) {
		snprintf(path, sizeof(path), "/txn/%s.%" PRIu64 "/description", session_id, txn_id);
		PUTBUFFER(path, info->description);
	}

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
	return CVMCACHE_STATUS_NOSUPPORT;
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

	uuid_t uu;
	uuid_generate(uu);
	uuid_unparse(uu, session_id);

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

	printf("using session id %s\n", session_id);

	ENSURE_DIR("/txn");

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
	callbacks.capabilities = CVMCACHE_CAP_REFCOUNT;

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
				printf("... asking clients to release nested catalogs\n");
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
