#define _GNU_SOURCE
#define _LARGEFILE_SOURCE
#define _LARGEFILE64_SOURCE
#define _FILE_OFFSET_BITS 64

#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdint.h>
#include <inttypes.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h> 

#define NUM_THREADS 16

volatile uint64_t nl_count;
volatile ssize_t buf_size;

void* count_nl(void* argument) {
	size_t n = 0;
	char* str = (char*)argument;
	for (size_t i = 0; i < buf_size; ++i)
		if (str[i] == '\n')
			n++;
	return (void*)(n);
}

void* count_nla(void* argument, uint32_t count) {
	size_t n = 0;
	char* str = (char*)argument;
	for (size_t i = 0; i < count; ++i)
		if (str[i] == '\n')
			n++;
	return (void*)(n);
}

int main(int argc, char** argv) {
	if (argc < 2)
		return 1;
	/* Preallocate buffer */
	ssize_t buf_read;
	uint32_t t;
	pthread_t threads[NUM_THREADS];
	uint64_t total_read = 0;
	int fd, i;
	if (argc >= 3)
		buf_size = 262144 * atoi(argv[2]);
	else
		buf_size = 1048576;
	fprintf(stderr, "Allocating buffer of %" PRIu64 " bytes\n", buf_size * NUM_THREADS);
	uint8_t* buf = (uint8_t*)malloc(buf_size * NUM_THREADS);
	/* Open file */
	fprintf(stderr, "Opening file %s\n", argv[1]);
	if ((fd = open(argv[1], O_RDONLY)) < 0) {
		free(buf);
		perror(argv[1]);
		return 1;
	}
	/* Count lines - expected 1267830474 for 2018-09-26-1537981537-rdns.json */
	fprintf(stderr, "Starting line count using %u threads\n", NUM_THREADS);
	nl_count = 0;
	while ((buf_read = read(fd, buf, buf_size * NUM_THREADS)) > 0) {
		if (buf_read == (buf_size * NUM_THREADS)) {
			for (i = 0; i < NUM_THREADS; i++)
				pthread_create(&threads[i], NULL, count_nl, (void*)(buf + (buf_size * i)));
			for (i = 0; i < NUM_THREADS; i++) {
				pthread_join(threads[i], (void**)&t);
				nl_count += t;
			}
		}
		else {
			/* Finish one-thread */
			nl_count += (size_t)(count_nla((void*)buf, buf_read));
		}
		/* Report progress */
		total_read += buf_read;
		if (total_read % (100 * 1048576) == 0)
			fprintf(stderr, "\r  %g MiB        \r", (total_read / 1048576.0));
	}
	fprintf(stderr, "\n");
	if (buf_read < 0) {
		perror(argv[1]);
	}
	/* Close file */
	fprintf(stderr, "Closing\n");
	if (close(fd) < 0) {
		perror(argv[1]);
	}
	free(buf);
	printf("%" PRIu64, nl_count);
	return 0;
}
