#ifndef COMMON_H
#define COMMON_H

#include <assert.h>
#include <string.h>
#include <stdarg.h>
#include <math.h>
#include "Log.h"
#include "Platform.h"

#define KB 1000
#define MB 1000000

#define MEMCMP(b1, l1, b2, l2) \
 ((l1) == (l2) && memcmp((b1), (b2), l1) == 0)

#define SIZE(a) \
 (sizeof((a)) / sizeof((a[0])))

#define ASSERT_FAIL() \
 assert(false)

#define STOP_FAIL(msg, code) \
{ \
	Log_SetTarget(LOG_TARGET_STDERR|LOG_TARGET_FILE); \
	Log_Message(msg); \
	_exit(code); \
}

#define RESTART(msg) \
{ \
	Log_Message(msg); \
	_exit(2); \
}

#define CS_INT_SIZE(int_type) \
 ((size_t)(0.30103 * sizeof(int_type) * 8) + 2 + 1)

#define ABS(a)		((a) > 0 ? (a) : (-1*(a)))
#define MIN(a, b)	((a) < (b) ? (a) : (b))
#define MAX(a, b)	((a) > (b) ? (a) : (b))

inline bool Xor(bool a, bool b, bool c)
{
	return (((int)a + (int)b + (int)c) == 1);
}

inline bool Xor(bool a, bool b)
{
	return Xor(a, b, false);
}

inline unsigned numlen(int n)
{
	return n == 0 ? 1 : (unsigned) floor(log10((float)n) + 1);
}

void* alloc(int num, int size = 1);

int64_t strntoint64(const char* buffer, int length, unsigned* nread);
uint64_t strntouint64(const char* buffer, int length, unsigned* nread);
char* strnchr(const char* s, int c, size_t len);
char* strrep(char* s, char src, char dst);

const char* rprintf(const char* format, ...);

bool del(const char* wc); // wildcard support
bool isdir(const char* path);

uint64_t gen_uuid();
void randseed();
int randint(int min, int max);

void blocksigs();

int snreadf(char* buffer, unsigned size, const char* format, ...);
int vsnreadf(char* buffer, unsigned size, const char* format, va_list ap);

int snwritef(char* buffer, unsigned size, const char* format, ...);
int vsnwritef(char* buffer, unsigned size, const char* format, va_list ap);

bool chuser(const char *username);

uint32_t checksum(const char* buffer, unsigned length);

#endif
