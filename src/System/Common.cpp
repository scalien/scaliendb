#include "Common.h"
#include <stdio.h>
#include <string>
#include <stdlib.h>
#include <sys/stat.h>
#include <signal.h>
#include "ByteString.h"
#ifdef _WIN32
#include "process.h"
#endif

void* alloc(int num, int size)
{
	if (num == 0 || size == 0)
		return NULL;
		
	return malloc(num * size);
}

int64_t strntoint64(const char* buffer, int length, unsigned* nread)
{
	bool	neg;
	long	i, digit;
	int64_t	n;
	char	c;

	if (buffer == NULL || length < 1)
	{
		*nread = 0;
		return 0;
	}
	
	n = 0;
	i = 0;
	c = *buffer;
	
#define ADVANCE()	i++; c = buffer[i];
	if (c == '-')
	{
		neg = true;
		ADVANCE();
	}
	else
		neg = false;
	
	while (i < length && c >= '0' && c <= '9')
	{
		digit = c - '0';
		n = n * 10 + digit;
		ADVANCE();
	}
#undef ADVANCE
	
	if (neg && i == 1)
	{
		*nread = 0;
		return 0;
	}
	
	*nread = i;

	if (neg)
		return -n;
	else
		return n;
}

uint64_t strntouint64(const char* buffer, int length, unsigned* nread)
{
	long		i, digit;
	uint64_t	n;
	char		c;

	if (buffer == NULL || length < 1)
	{
		*nread = 0;
		return 0;
	}

	n = 0;
	i = 0;
	c = *buffer;
	
#define ADVANCE()	i++; c = buffer[i];	
	while (i < length && c >= '0' && c <= '9')
	{
		digit = c - '0';
		n = n * 10 + digit;
		ADVANCE();
	}
#undef ADVANCE
	
	*nread = i;

	return n;
}

char* strnchr(const char* s, int c, size_t len)
{
	size_t	i;
	
	for (i = 0; i < len && s[i]; s++)
		if (s[i] == c) return (char*) (s + i);
	
	if (c == 0 && i < len && s[i] == 0)
		return (char*) (s + i);
	
	return NULL;
}

char* strrep(char* s, char src, char dst)
{
	unsigned len;
	unsigned i;

	len = strlen(s);
	for (i = 0; i < len; i++)
		if (s[i] == src)
			s[i] = dst;

	return s;
}

const char* rprintf(const char* format, ...)
{
	static char buffer[8*1024];
	va_list		ap;
	
	va_start(ap, format);
	vsnprintf(buffer, sizeof(buffer), format, ap);
	va_end(ap);
	
	return buffer;
}

bool del(const char* wc)
{
	char buf[4096];
	
	strcpy(buf, wc);
#ifdef _WIN32
	strrep(buf, '/', '\\');
	return (_spawnlp(_P_WAIT, "cmd", "/c", "del", buf, NULL) == 0);
#else
	snprintf(buf, SIZE(buf), "rm %s", wc);
	return (system(buf) == 0);
#endif
}

bool isdir(const char* path)
{
	struct stat s;
	if (stat(path, &s) != 0)
		return false;
	if (s.st_mode & S_IFDIR)
		return true;
	return false;
}

int randint(int min, int max)
{
	int		rnd;
	int		interval;
	
	assert(min < max);

	interval = max - min;
#ifdef _WIN32
#define random rand
#endif
	rnd = (int)(random() / (float) RAND_MAX * interval + 0.5);
	return rnd + min;
}


void blocksigs()
{
#ifdef _WIN32
	// dummy
#else
	sigset_t	sigmask;
	
	// block all signals
	sigfillset(&sigmask);
	pthread_sigmask(SIG_SETMASK, &sigmask, NULL);
#endif
}

/*
 * snreadf/vsnreadf is a simple sscanf replacement for working with non-null
 * terminated strings
 *
 * specifiers:
 * %%					reads the '%' character
 * %c (char)			reads a char
 * %d (int)				reads a signed int
 * %u (unsigned)		reads an unsigned int
 * %I (int64_t)			reads a signed int64
 * %U (uint64_t)		reads an unsigned int64
 * %M (ByteString* bs)	reads a %u:<%u long buffer> into bs.length and copies
 *						the buffer into bs.buffer taking into account bs.size
 * %N (ByteString* bs)	same as %M but does not copy the contents, only sets
 *						bs.buffer and sets bs.length = bs.size to the prefix
 *
 * returns the number of bytes read from buffer, or -1 if the format string did
 * not match the buffer
 */

#if 0

int snreadf(char* buffer, unsigned length, const char* format, ...)
{
	int			read;
	va_list		ap;
	
	va_start(ap, format);
	read = vsnreadf(buffer, length, format, ap);
	va_end(ap);
	
	return read;
}

int vsnreadf(char* buffer, unsigned length, const char* format, va_list ap)
{
	char*		c;
	int*		d;
	unsigned*	u;
	int64_t*	i64;
	uint64_t*	u64;
	ByteString*	bs;
	unsigned	n;
	int			read;

#define ADVANCE(f, b)	{ format += f; buffer += b; length -= b; read += b; }
#define EXIT()			{ return -1; }
#define REQUIRE(r)		{ if (length < r) EXIT() }

	read = 0;

	while(format[0] != '\0')
	{
		if (format[0] == '%')
		{
			if (format[1] == '\0')
				EXIT(); // % cannot be at the end of the format string
			
			if (format[1] == '%') // %%
			{
				REQUIRE(1);
				if (buffer[0] != '%') EXIT();
				ADVANCE(2, 1);
			} else if (format[1] == 'c') // %c
			{
				REQUIRE(1);
				c = va_arg(ap, char*);
				*c = buffer[0];
				ADVANCE(2, 1);
			} else if (format[1] == 'd') // %d
			{
				d = va_arg(ap, int*);
				*d = strntoint64(buffer, length, &n);
				if (n < 1) EXIT();
				ADVANCE(2, n);
			} else if (format[1] == 'u') // %u
			{
				u = va_arg(ap, unsigned*);
				*u = strntouint64(buffer, length, &n);
				if (n < 1) EXIT();
				ADVANCE(2, n);
			} else if (format[1] == 'I') // %I
			{
				i64 = va_arg(ap, int64_t*);
				*i64 = strntoint64(buffer, length, &n);
				if (n < 1) EXIT();
				ADVANCE(2, n);
			} else if (format[1] == 'U') // %U
			{
				u64 = va_arg(ap, uint64_t*);
				*u64 = strntouint64(buffer, length, &n);
				if (n < 1) EXIT();
				ADVANCE(2, n);
			}
			else if (format[1] == 'M') // %M
			{
				bs = va_arg(ap, ByteString*);
				// read the length prefix
				bs->length = strntouint64(buffer, length, &n);
				if (n < 1) EXIT();
				if (bs->length > bs->size)
				{
					bs->length = 0;
					EXIT();
				}
				ADVANCE(0, n);
				// read the ':'
				REQUIRE(1);
				if (buffer[0] != ':') EXIT();
				ADVANCE(0, 1);
				// read the message body
				REQUIRE(bs->length);
				memcpy(bs->buffer, buffer, bs->length);
				ADVANCE(2, bs->length);
			}
			else if (format[1] == 'N') // %N
			{
				bs = va_arg(ap, ByteString*);
				// read the length prefix
				bs->length = strntouint64(buffer, length, &n);
				if (n < 1) EXIT();
				ADVANCE(0, n);
				// read the ':'
				REQUIRE(1);
				if (buffer[0] != ':') EXIT();
				ADVANCE(0, 1);
				// read the message body
				REQUIRE(bs->length);
				bs->buffer = buffer;
				bs->size = bs->length;
				ADVANCE(2, bs->length);
			}
			else
			{
				ASSERT_FAIL();
			}
		}
		else
		{
			REQUIRE(1);
			if (buffer[0] != format[0]) EXIT();
			ADVANCE(1, 1);
		}
	}

	if (format[0] != '\0')
		return -1;
	
	return read;

#undef ADVANCE
#undef EXIT
#undef REQUIRE
}

/*
 * snwritef/vsnwritef is a simple snprintf replacement for working with
 * non-null terminated strings
 *
 * supported specifiers:
 * %%						prints the '%' character
 * %c (char)				prints a char
 * %d (int)					prints a signed int
 * %u (unsigned)			prints an unsigned int
 * %I (int64_t)				prints a signed int64
 * %U (uint64_t)			prints an unsigned int64
 * %s (char* p)				copies strlen(p) bytes from p to the output buffer
 * %B (ByteString* bs)		copies bs->length length bytes from bs.buffer to
 *							the output buffer, irrespective of \0 chars
 * %M (ByteString* bs)		same as %u:%B with
 *							(bs->length, bs)
 *
 * snwritef does not null-terminate the resulting buffer
 * returns the number of bytes required or written, or -1 on error
 * (if size bytes were not enough, snwritef writes size bytes and returns the
 * number of bytes that would have been required)
 */

#endif

int snwritef(char* buffer, unsigned size, const char* format, ...)
{
	int			required;
	va_list		ap;	
	
	va_start(ap, format);
	required = vsnwritef(buffer, size, format, ap);
	va_end(ap);
	
	return required;
}

int vsnwritef(char* buffer, unsigned size, const char* format, va_list ap)
{
	char		c;
	int			d;
	unsigned	u;
	int			n;
	int64_t		i64;
	uint64_t	u64;
	char*		p;
	unsigned	length;
	ByteString*	bs;
	int			required;
	char		local[64];
	bool		ghost;

#define ADVANCE(f, b)	{ format += f; if (!ghost) { buffer += b; size -= b; } }
#define EXIT()			{ return -1; }
#define REQUIRE(r)		{ required += r; if (size < (unsigned)r) ghost = true; }

	ghost = false;
	required = 0;

	while(format[0] != '\0')
	{
		if (format[0] == '%')
		{
			if (format[1] == '\0')
				EXIT(); // % cannot be at the end of the format string
			
			if (format[1] == '%') // %%
			{
				REQUIRE(1);
				if (!ghost) buffer[0] = '%';
				ADVANCE(2, (ghost ? 0 : 1));
			} else if (format[1] == 'c') // %c
			{
				REQUIRE(1);
				c = va_arg(ap, int);
				if (!ghost) buffer[0] = c;
				ADVANCE(2, 1);
			} else if (format[1] == 'd') // %d
			{
				d = va_arg(ap, int);
				n = snprintf(local, sizeof(local), "%d", d);
				if (n < 0) EXIT();
				REQUIRE(n);
				if (ghost) n = size;
				memcpy(buffer, local, n);
				ADVANCE(2, n);
			} else if (format[1] == 'u') // %u
			{
				u = va_arg(ap, unsigned);
				n = snprintf(local, sizeof(local), "%u", u);
				if (n < 0) EXIT();
				REQUIRE(n);
				if (ghost) n = size;
				memcpy(buffer, local, n);
				ADVANCE(2, n);
			} else if (format[1] == 'I') // %I to print an int64_t 
			{
				i64 = va_arg(ap, int64_t);
				n = snprintf(local, sizeof(local), "%" PRIi64 "", i64);
				if (n < 0) EXIT();
				REQUIRE(n);
				if (ghost) n = size;
				memcpy(buffer, local, n);
				ADVANCE(2, n);
			} else if (format[1] == 'U') // %U tp print an uint64_t
			{
				u64 = va_arg(ap, uint64_t);
				n = snprintf(local, sizeof(local), "%" PRIu64 "", u64);
				if (n < 0) EXIT();
				REQUIRE(n);
				if (ghost) n = size;
				memcpy(buffer, local, n);
				ADVANCE(2, n);
			} else if (format[1] == 's') // %s to print a string
			{
				p = va_arg(ap, char*);
				length = strlen(p);
				REQUIRE(length);
				if (ghost) length = size;
				memcpy(buffer, p, length);
				ADVANCE(2, length);
			} else if (format[1] == 'B') // %B to print a buffer
			{
				bs = va_arg(ap, ByteString*);
				p = bs->Buffer();
				length = bs->Length();
				REQUIRE(length);
				if (ghost) length = size;
				memcpy(buffer, p, length);
				ADVANCE(2, length);
			} else if (format[1] == 'M') // %M to print a message
			{
				bs = va_arg(ap, ByteString*);
				n = snprintf(local, sizeof(local), "%u:", bs->Length());
				if (n < 0) EXIT();
				REQUIRE(n);
				if (ghost) n = size;
				memcpy(buffer, local, n);
				ADVANCE(0, n);
				REQUIRE(bs->Length());
				length = bs->Length();
				if (ghost) length = size;
				memmove(buffer, bs->Buffer(), length);
				ADVANCE(2, length);
			}
			else
			{
				ASSERT_FAIL();
			}
		}
		else
		{
			REQUIRE(1);
			if (!ghost) buffer[0] = format[0];
			ADVANCE(1, (ghost ? 0 : 1));
		}
	}
	
	return required;

#undef ADVANCE
#undef EXIT
#undef REQUIRE
}
