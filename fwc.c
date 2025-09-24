#define _GNU_SOURCE
#define _LARGEFILE_SOURCE
#define _LARGEFILE64_SOURCE
#define _FILE_OFFSET_BITS 64
#define __STDC_LIMIT_MACROS

#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdbool.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h> 
#include <limits.h>
#include <errno.h>
#include <string.h>

typedef struct {
	char* buf;
	ssize_t buf_len;
	/**
	 * 0 - empty/completed 
	 * 1 - loading
	 * 2 - loaded
	 * 3 - processing
	 */
	int status;
} buf_unit;

#define BUF_COMPLETED 0
#define BUF_LOADING 1
#define BUF_LOADED 2
#define BUF_PROCESSING 3

volatile bool running = true;
volatile size_t starving_ms = 0;
volatile size_t resting_ms = 0;
buf_unit* units;
long num_units;
pthread_mutex_t units_lock;

buf_unit* alloc_buf_units(long num, ssize_t buf_size) {
	buf_unit* units = (buf_unit*) calloc(num, sizeof(buf_unit));
	for (long i = 0; i < num; i++) {
		units[i].buf = (char*) malloc(buf_size);
	}
	return units;
}

void* free_buf_units(buf_unit* units, long num) {
	for (long i = 0; i < num; i++) {
		free(units[i].buf);
	}
	free(units);
}

void* count_nl(void* argument) {
	size_t n = 0;
	
	while (running) {
		buf_unit* working_unit = NULL;
		/* Acquire new working unit, if any available */
		pthread_mutex_lock(&units_lock);
		for (int i = 0; i < num_units; i++) {
			if (units[i].status == BUF_LOADED) {
				working_unit = &units[i];
				working_unit->status = BUF_PROCESSING;
				break;
			}
		}
		pthread_mutex_unlock(&units_lock);
		if (NULL == working_unit) {
			starving_ms++;
			usleep(1000);
			continue;
		}
		/* Process working unit */
		for (size_t i = 0; i < working_unit->buf_len; ++i) {
			if (working_unit->buf[i] == '\n') {
				n++;
			}
		}
		/* Mark as completed */
		pthread_mutex_lock(&units_lock);
		working_unit->status = BUF_COMPLETED;
		pthread_mutex_unlock(&units_lock);
	}

	return (void*)(n);
}

int count(const char* path, long num_threads, ssize_t buf_size, uint64_t* nl_count, bool verbose) {
	ssize_t buf_read = 0, total_read = 0, last_reported_total_read = 0;
	size_t t;
	pthread_t threads[num_threads];
	int fd, i, gec = 0;
	
	if ((fd = open(path, O_RDONLY)) < 0) {
		perror(path);
		return errno;
	}
	
	units = alloc_buf_units(num_units, buf_size);
	pthread_mutex_init(&units_lock, NULL);
	
	for (i = 0; i < num_threads; i++) {
		int ec = pthread_create(&threads[i], NULL, count_nl, NULL);
		if (ec) {
			gec = ec;
			perror(path);
			running = false;
			break;
		}
	}
	
	while (running) {
		buf_unit* working_unit = NULL;
		pthread_mutex_lock(&units_lock);
		for (i = 0; i < num_units; i++) {
			if (units[i].status == BUF_COMPLETED) {
				working_unit = &units[i];
				working_unit->status = BUF_LOADING;
				break;
			}
		}
		pthread_mutex_unlock(&units_lock);
		if (NULL == working_unit) {
			resting_ms++;
			usleep(1000);
			continue;
		}
		buf_read = read(fd, working_unit->buf, buf_size);
		if (buf_read <= 0) {
			if (buf_read < 0) {
				perror(path);
			}
			break;
		}
		total_read += buf_read;
		if (verbose && total_read - last_reported_total_read >= (100 * 1048576)) {
			fprintf(stderr, "\r  %.3f MiB (%zu ms resting, %zu ms starving)       \r", (total_read / 1048576.0), resting_ms, starving_ms);
			last_reported_total_read = total_read;
		}
		pthread_mutex_lock(&units_lock);
		working_unit->buf_len = buf_read;
		working_unit->status = BUF_LOADED;
		pthread_mutex_unlock(&units_lock);
	}
	
	running = false;
	for (i = 0; i < num_threads; i++) {
		int ec = pthread_join(threads[i], (void**)&t);
		if (ec) {
			if (!gec) {
				gec = ec;
			}
			perror(path);
		} else {
			*nl_count += t;
		}
		memset(&threads[i], 0, sizeof(pthread_t));
	}
	
	if (verbose) {
		fprintf(stderr, "\n");
	}
	if (buf_read < 0) {
		perror(path);
	}
	if (close(fd) < 0) {
		perror(path);
	}
	
	pthread_mutex_destroy(&units_lock);
	free_buf_units(units, num_units);
	return gec;
}

int strtosize(const char* input, ssize_t* dest) {
	char* endptr;
	
	*dest = strtoull(input, &endptr, 10);
	switch (endptr[0]) {
		case '\0':
			break;
		case 'G':
		case 'g':
			*dest *= 1024;
			// fallthrough;
		case 'M':
		case 'm':
			*dest *= 1024;
			// fallthrough;
		case 'K':
		case 'k':
			*dest *= 1024;
			break;
		default:
			fprintf(stderr, "Invalid unit '%s'\n", endptr);
			return EINVAL;
	}
	return 0;
}

int main(int argc, char** argv) {
	int opt;
	bool verbose = false;
	bool human_readable = false;
	ssize_t buf_size = 0;
	uint64_t nl_count = 0;
	long num_threads = 0;
	
	while ((opt = getopt(argc, argv, "b:hn:v")) != -1) {
		switch (opt) {
			case 'b':
				if (strtosize(optarg, &buf_size)) {
					return EINVAL;
				}
				break;
		    case 'h':
				human_readable = true;
				break;
			case 'n':
			   num_threads = atol(optarg);
			   break;
			case 'v':
			   verbose = true;
			   break;
			default: /* '?' */
			   fprintf(stderr, "Usage: %s [-b buffer_size] [-n num_threads] [-v] file_name\n", argv[0]);
			   return EINVAL;
		}
	}
	if (optind >= argc) {
		fprintf(stderr, "Expected argument after options\n");
		return EINVAL;
	}
	if (0 == num_threads) {
		num_threads = sysconf(_SC_NPROCESSORS_ONLN);
	}
	if (0 == buf_size) {
		buf_size = 1048576;
	}
	if ((buf_size <= 0) || (buf_size > SSIZE_MAX / num_threads)) {
		fprintf(stderr, "Invalid buffer size\n");
		return ENOMEM;
	}
	num_units = num_threads * 2;
	int retval = count(argv[optind], num_threads, buf_size, &nl_count, verbose);
	if (0 == retval) {
		if (human_readable) {
			printf("%'" PRIu64, nl_count);
		} else {
			printf("%" PRIu64, nl_count);
		}
	}
	return retval;
}
