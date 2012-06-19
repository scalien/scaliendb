#ifndef PLATFORM_H
#define PLATFORM_H

/*
===============================================================================================

 Include this instead of platform specific .h files

===============================================================================================
*/

#ifdef _WIN32 // start Windows

#ifdef _WIN64
#define PLATFORM_STRING     "Windows 64-bit"
#else
#define PLATFORM_STRING     "Windows 32-bit"
#endif

// conversion to smaller type, possible loss of data
#pragma warning(disable: 4244)
// conversion from size_t to smaller type, possible loss of data
#pragma warning(disable: 4267)
// 'this' : used in base member initializer list
#pragma warning(disable: 4355)
// conditional expression is constant
#pragma warning(disable: 4127)

#include <stddef.h>     // for intptr_t
#include <stdio.h>

// VC++ 8.0 is not C99-compatible
typedef __int8              int8_t;
typedef __int16             int16_t;
typedef __int32             int32_t;
typedef __int64             int64_t;

typedef unsigned __int8     uint8_t;
typedef unsigned __int16    uint16_t;
typedef unsigned __int32    uint32_t;
typedef unsigned __int64    uint64_t;

typedef intptr_t            ssize_t;

// 64bit compatible format string specifiers according to this document
// http://msdn.microsoft.com/en-us/library/tcxf1dw6.aspx
#define PRIu64              "I64i"
#define PRIi64              "I64i"

// This needs to be defined so that random becomes more random
// http://msdn.microsoft.com/en-us/library/sxtz2fa8%28VS.80%29.aspx
#define _CRT_RAND_S

#define snprintf            _snprintf
#define strdup              _strdup
#define strncasecmp         _strnicmp
#define localtime_r(t, tm)  localtime_s(tm, t)
#define __func__            __FUNCTION__
#define THREAD_LOCAL        __declspec(thread)

#else // end Windows, start Unix

#ifdef PLATFORM_LINUX
#ifdef __amd64__
#define PLATFORM_STRING     "Linux 64-bit"
#else
#define PLATFORM_STRING     "Linux 32-bit"
#endif
#endif

#ifdef PLATFORM_DARWIN
#ifdef __amd64__
#define PLATFORM_STRING     "Darwin 64-bit"
#else
#define PLATFORM_STRING     "Darwin 32-bit"
#endif
#endif

#include <stdint.h>
#include <inttypes.h>
#include <unistd.h>
#include <stdlib.h>

#define THREAD_LOCAL        __thread

#endif // end Unix

#include <math.h>

#endif
