#ifndef FORMATTING_H
#define FORMATTING_H

#include <stdarg.h>

/*
===============================================================================================

 Formatting: our own printf() replacement supporting %U, %I, %R, %#R, %B, %#B, etc.

===============================================================================================
*/

int Readf(char* buffer, unsigned size, const char* format, ...);
int VReadf(char* buffer, unsigned size, const char* format, va_list ap);

int Writef(char* buffer, unsigned size, const char* format, ...);
int VWritef(char* buffer, unsigned size, const char* format, va_list ap);

#endif
