#include "test.h"

#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>

#define TEST_HEAD(testfn, testname) \
	{ \
		if (!testname) { \
			testname = "<NONAMETEST>"; \
		} \
		if (!testfn) { \
			printf("test: %s failed, no test function!\n", testname); \
			return 1; \
		} \
		printf("test: %s begin ------------------------\n", testname); \
	}

int
test(testfn_t testfn, const char *testname)
{
	int res;

	TEST_HEAD(testfn, testname);
	
	res = testfn();

	if (res == 0) {
		printf("test: %s succeeded. -------------------\n", testname);
		return 0;
	} else {
		printf("test: %s failed!!!!!!!!!!!!!!!!!!!!!!!!\n", testname);
		return 1;
	}
}

int
test_iter(testfn_t testfn, const char *testname, unsigned long niter)
{
	unsigned long i;
	int res = 0;

	TEST_HEAD(testfn, testname);

	for (i = 0; i < niter; i++) {
		res += testfn();
	}

	if (res == 0) {
		printf("test: %s succeeded.\n", testname);
		return 0;
	} else {
		printf("test: %s failed!\n", testname);
		return 1;
	}
}

static long
elapsed_usec(struct timeval start, struct timeval end)
{
	return ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec - start.tv_usec);	
}

#define elapsed_msec(start, end) (elapsed_usec((start), (end)) / 1000)
#define elapsed_sec(start, end) (elapsed_usec((start), (end)) / 1000000)

int
test_time(testfn_t testfn, const char *testname)
{
	int res;
	struct timeval start, end;
	
	TEST_HEAD(testfn, testname);

	gettimeofday(&start, NULL);
	res = testfn();
	gettimeofday(&end, NULL);

	if (res == 0) {
		printf("test: %s succeeded (elapsed time = %ld ms).\n", testname, elapsed_msec(start, end));
		return 0;
	} else {
		printf("test: %s failed (elapsed time = %ld ms)!\n", testname, elapsed_msec(start, end));
		return 1;
	}
}

int
test_iter_time(testfn_t testfn, const char *testname, unsigned long niter)
{
	unsigned long i;
	int res = 0;
	struct timeval start, end;
	
	TEST_HEAD(testfn, testname);

	gettimeofday(&start, NULL);
	for (i = 0; i < niter; i++) {
		res += testfn();
	}
	gettimeofday(&end, NULL);

	if (res == 0) {
		printf("test: %s succeeded (elapsed time = %ld ms).\n", testname, elapsed_msec(start, end));
		return 0;
	} else {
		printf("test: %s failed (elapsed time = %ld ms)!\n", testname, elapsed_msec(start, end));
		return 1;
	}
}

int
test_eval(const char *ident, int result)
{
	printf("test: %s %s (%d)\n", ident, result ? "failed." : "succeeded.", result);

#ifdef _WIN32
#ifdef _CONSOLE
	system("pause");
#endif
#endif
	
	return result;
}

int
test_system(const char *cmdline)
{
	int res;

	printf("test_system: %s begin ----------------------\n", cmdline);
	
	res = system(cmdline);
	if (res == 0) {
		printf("test_system: %s succeeded. -------------------\n", cmdline);
	} else {
		printf("test_system: %s failed!!!!!!!!!!!!!!!!!!!!!!!!\n", cmdline);
	}
	
	return res;
}

int
test_names_parse(testfn_t *test_functions, char *test_names, int *names, int size)
{
	(void) test_functions;
	(void) size;
	int i = 0;
	char *p = test_names;
	while (*p) {
		names[i++] = p - test_names;
		char *comma = strstr(test_names, ",");
		if (comma) {
			*comma = '\0';
			p = comma + 1;
			while (*p && *p <= ' ') p++;
		} else
			break;
	}

	return 0;
}
