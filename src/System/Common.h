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

#define KB      1000
#define MB      1000000

#define KiB     1024
#define MiB     (1024*1024)

#define YES     'Y'
#define NO      'N'

unsigned        NumDigits(int n);
unsigned        NumDigits64(uint64_t n);

const char*     HumanBytes_(uint64_t bytes, char buf[5]);
#define         HumanBytes(bytes) HumanBytes_(bytes, (char*) alloca(5))

const char*     SIBytes_(uint64_t bytes, char buf[5]);
#define         SIBytes(bytes) SIBytes_(bytes, (char*) alloca(5))

int64_t         BufferToInt64(const char* buffer, unsigned length, unsigned* nread);
uint64_t        BufferToUInt64(const char* buffer, unsigned length, unsigned* nread);

char*           FindInBuffer(const char* buffer, unsigned length, char c);
char*           FindInCString(const char* s, char c);

void            ReplaceInBuffer(char* buffer, unsigned length, char src, char dst);
void            ReplaceInCString(char* s, char src, char dst);

const char*     StaticPrint(const char* format, ...);

bool            Delete(const char* path); // supports wildcards

uint64_t        GenerateGUID();
void            SeedRandom();
void            SeedRandomWith(uint64_t seed);
int             RandomInt(int min, int max);
void            RandomBuffer(char* buffer, unsigned length);

void            BlockSignals();
bool            ChangeUser(const char *username);

uint32_t        ChecksumBuffer(const char* buffer, unsigned length);

uint64_t        ToLittle64(uint64_t num);
uint32_t        ToLittle32(uint32_t num);
uint32_t        ToLittle16(uint32_t num);

uint64_t        FromLittle64(uint64_t num);
uint32_t        FromLittle32(uint32_t num);
uint32_t        FromLittle16(uint32_t num);

#endif
