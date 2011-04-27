#ifndef COMMON_H
#define COMMON_H

#include <string.h>
#include <stdarg.h>
#include <math.h>
#include "Platform.h"
#include "Macros.h"
#include "Formatting.h"
#include "Log.h"

class ReadBuffer;

/*
===============================================================================================

 Common: ...

===============================================================================================
*/

#define KB      1000
#define MB      1000000
#define GB      1000000000

#define KiB     1024
#define MiB     (1024*1024)
#define GiB     (1024*1024*1024)

#define YES     'Y'
#define NO      'N'

unsigned        NumDigits(int n);
unsigned        NumDigits64(uint64_t n);

const char*     HumanBytes(uint64_t bytes, char buf[5]);
#define         HUMAN_BYTES(bytes) HumanBytes(bytes, (char*) alloca(5))

const char*     SIBytes(uint64_t bytes, char buf[5]);
#define         SI_BYTES(bytes) SIBytes(bytes, (char*) alloca(5))

const char*     HumanTime(char buf[27]);
#define         HUMAN_TIME() HumanTime((char*) alloca(27))

int64_t         BufferToInt64(const char* buffer, unsigned length, unsigned* nread);
uint64_t        BufferToUInt64(const char* buffer, unsigned length, unsigned* nread);

int             UInt64ToBufferWithBase(char* buffer, unsigned length, uint64_t value, char base);

char*           FindInBuffer(const char* buffer, unsigned length, char c);
char*           RevFindInBuffer(const char* buffer, unsigned length, char c);
char*           FindInCString(const char* s, char c);

void            ReplaceInBuffer(char* buffer, unsigned length, char src, char dst);
void            ReplaceInCString(char* s, char src, char dst);

bool            RangeContains(ReadBuffer firstKey, ReadBuffer lastKey, ReadBuffer key);

const char*     StaticPrint(const char* format, ...);

uint64_t        GenerateGUID();
void            SeedRandom();
void            SeedRandomWith(uint64_t seed);
int             RandomInt(int min, int max);
void            RandomBuffer(char* buffer, unsigned length);

void            BlockSignals();
bool            ChangeUser(const char *username);
void            PrintStackTrace();
int             ShellExec(const char *cmdline);
uint64_t        GetProcessID();
uint64_t        GetTotalPhysicalMemory();
void            SetMemoryLimit(uint64_t limit);

uint32_t        ChecksumBuffer(const char* buffer, unsigned length);

uint64_t        ToLittle64(uint64_t num);
uint32_t        ToLittle32(uint32_t num);
uint32_t        ToLittle16(uint32_t num);

uint64_t        FromLittle64(uint64_t num);
uint32_t        FromLittle32(uint32_t num);
uint32_t        FromLittle16(uint32_t num);

uint32_t        NextPowerOfTwo(uint32_t x);

#endif
