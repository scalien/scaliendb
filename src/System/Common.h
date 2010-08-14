#ifndef COMMON_H
#define COMMON_H

#include <assert.h>
#include <string.h>
#include <stdarg.h>
#include <math.h>
#include "Platform.h"
#include "Macros.h"
#include "Formatting.h"
#include "Log.h"

#define KB 1000
#define MB 1000000

unsigned		NumDigits(int n);

int64_t			BufferToInt64(const char* buffer, unsigned length, unsigned* nread);
uint64_t		BufferToUInt64(const char* buffer, unsigned length, unsigned* nread);

char*			FindInBuffer(const char* buffer, unsigned length, char c);
char*			FindInCString(const char* s, char c);

void			ReplaceInBuffer(char* buffer, unsigned length, char src, char dst);
void			ReplaceInCString(char* s, char src, char dst);

const char*		StaticPrint(const char* format, ...);

bool			Delete(const char* path); // supports wildcards
bool			IsDirectory(const char* path);
int64_t			GetFreeDiskSpace(const char* path);	// returns in bytes

uint64_t		GenerateGUID();
void			SeedRandom();
int				RandomInt(int min, int max);

void			BlockSignals();
bool			ChangeUser(const char *username);

uint32_t		ChecksumBuffer(const char* buffer, unsigned length);

#endif
