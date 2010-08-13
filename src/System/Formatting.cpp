#include "Formatting.h"

/*
 * Readf is a simple sscanf replacement for working with non-null
 * terminated strings
 *
 * specifiers:
 * %%					reads the '%' character
 * %c (char)			reads a char
 * %d (int)				reads a signed int
 * %u (unsigned)		reads an unsigned int
 * %I (int64_t)			reads a signed int64
 * %U (uint64_t)		reads an unsigned int64
 * %M (Buffer* b)		reads a %u:<%u long buffer> into b.length and copies
 *						the buffer into b.buffer calling Allocate()
 *
 * returns the number of bytes read from buffer, or -1 if the format string did
 * not match the buffer
 */

int Readf(char* buffer, unsigned length, const char* format, ...)
{
	int			read;
	va_list		ap;
	
	va_start(ap, format);
	read = vsnreadf(buffer, length, format, ap);
	va_end(ap);
	
	return read;
}

int VReadf(char* buffer, unsigned length, const char* format, va_list ap)
{
	char*			c;
	int*			d;
	unsigned*		u;
	int64_t*		i64;
	uint64_t*		u64;
	ReadBuffer*		rb;
	unsigned		n, l;
	int				read;

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
				*d = BufferToInt64(buffer, length, &n);
				if (n < 1) EXIT();
				ADVANCE(2, n);
			} else if (format[1] == 'u') // %u
			{
				u = va_arg(ap, unsigned*);
				*u = BufferToUInt64(buffer, length, &n);
				if (n < 1) EXIT();
				ADVANCE(2, n);
			} else if (format[1] == 'I') // %I
			{
				i64 = va_arg(ap, int64_t*);
				*i64 = BufferToInt64(buffer, length, &n);
				if (n < 1) EXIT();
				ADVANCE(2, n);
			} else if (format[1] == 'U') // %U
			{
				u64 = va_arg(ap, uint64_t*);
				*u64 = BufferToUInt64(buffer, length, &n);
				if (n < 1) EXIT();
				ADVANCE(2, n);
			}
			else if (format[1] == 'M') // %M
			{
				rb = va_arg(ap, ReadBuffer*);
				// read the length prefix
				l = BufferToUInt64(buffer, length, &n);
				if (n < 1) EXIT();
				ADVANCE(0, n);
				// read the ':'
				REQUIRE(1);
				if (buffer[0] != ':') EXIT();
				ADVANCE(0, 1);
				// read the message body
				REQUIRE(l);
				rb->SetBuffer(buffer);
				rb->SetLength(l);
				ADVANCE(2, rb->GetLength());
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
 * %B (Buffer* b)			copies bs->length length bytes from bs.buffer to
 *							the output buffer, irrespective of \0 chars
 * %M (Buffer* bs)			same as %u:%B with
 *							(bs->length, bs)
 *
 * snwritef does not null-terminate the resulting buffer
 * returns the number of bytes required or written, or -1 on error
 * (if size bytes were not enough, snwritef writes size bytes and returns the
 * number of bytes that would have been required)
 */

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
	char			c;
	int				d;
	unsigned		u;
	int				n;
	int64_t			i64;
	uint64_t		u64;
	char*			p;
	unsigned		length;
	Buffer*			b;
	ReadBuffer*		rb;
	int				required;
	char			local[64];
	bool			ghost;

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
				b = va_arg(ap, Buffer*);
				p = b->GetBuffer();
				length = b->GetLength();
				REQUIRE(length);
				if (ghost) length = size;
				memcpy(buffer, p, length);
				ADVANCE(2, length);
			} else if (format[1] == 'M') // %M to print a message
			{
				rb = va_arg(ap, ReadBuffer*);
				n = snprintf(local, sizeof(local), "%u:", rb->GetLength());
				if (n < 0) EXIT();
				REQUIRE(n);
				if (ghost) n = size;
				memcpy(buffer, local, n);
				ADVANCE(0, n);
				REQUIRE(rb->GetLength());
				length = rb->GetLength();
				if (ghost) length = size;
				memmove(buffer, rb->GetBuffer(), length);
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
