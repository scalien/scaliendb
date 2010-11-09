#include "Formatting.h"
#include <assert.h>
#include <string.h>
#include <math.h>
#include <stdio.h>
#include "Platform.h"
#include "Buffers/Buffer.h"
#include "Buffers/ReadBuffer.h"

// this works the same as snprintf(buf, bufsize, "%" PRIu64, value) would do
int UInt64ToBuffer(char* buf, size_t bufsize, uint64_t value)
{
    char        tmp[CS_INT_SIZE(uint64_t)];
    unsigned    d;

    // special case
    if (value == 0)
    {
        if (bufsize == 0)
            return 1;
            
        if (bufsize > 0)
            buf[0] = '0';
        else
            buf[0] = 0;
            
        if (bufsize > 1)
            buf[1] = 0;

        return 1;
    }

    // write digits to reverse order in temp buffer
    d = 0;
    while (value > 0)
    {
        tmp[d] = '0' + value % 10;
        d += 1;
        value /= 10;
    }
    
    // copy the digits
    for (unsigned i = 0; i < d; i++)
    {
        if (i < bufsize)
            buf[i] = tmp[d - 1 - i];
    }

    // terminate with zero
    if (d < bufsize)
        buf[d] = 0;
    else
        buf[bufsize - 1] = 0;

    return d;
}

static int Int64ToBuffer(char* buf, size_t bufsize, int64_t value)
{
    if (value < 0)
    {
        if (bufsize == 0)
            return UInt64ToBuffer(buf, 0, (uint64_t) -value) + 1;
            
        if (bufsize == 1)
        {
            buf[0] = 0;
            return UInt64ToBuffer(buf, 0, (uint64_t) -value) + 1;
        }
        
        buf[0] = '-';
        return UInt64ToBuffer(buf + 1, bufsize - 1, (uint64_t) -value) + 1;
    }
    
    return UInt64ToBuffer(buf, bufsize, (uint64_t) value);
}

int UIntToBuffer(char* buf, size_t bufsize, unsigned value)
{
    char        tmp[CS_INT_SIZE(unsigned)];
    unsigned    d;
    
    // special case
    if (value == 0)
    {
        if (bufsize == 0)
            return 1;
            
        if (bufsize > 0)
            buf[0] = '0';
        else
            buf[0] = 0;
            
        if (bufsize > 1)
            buf[1] = 0;

        return 1;
    }

    // write digits to reverse order in temp buffer
    d = 0;
    while (value > 0)
    {
        tmp[d] = '0' + value % 10;
        d += 1;
        value /= 10;
    }
    
    // copy the digits
    for (unsigned i = 0; i < d; i++)
    {
        if (i < bufsize)
            buf[i] = tmp[d - 1 - i];
    }

    // terminate with zero
    if (d < bufsize)
        buf[d] = 0;
    else
        buf[bufsize - 1] = 0;

    return d;
}

int IntToBuffer(char* buf, size_t bufsize, int value)
{
    if (value < 0)
    {
        if (bufsize == 0)
            return UIntToBuffer(buf, 0, (unsigned) -value) + 1;
            
        if (bufsize == 1)
        {
            buf[0] = 0;
            return UIntToBuffer(buf, 0, (unsigned) -value) + 1;
        }
        
        buf[0] = '-';
        return UIntToBuffer(buf + 1, bufsize - 1, (unsigned) -value) + 1;
    }
    
    return UIntToBuffer(buf, bufsize, (unsigned) value);
}

/*
 * Readf is a simple sscanf replacement for working with non-null
 * terminated strings
 *
 * specifiers:
 * %%                       reads the '%' character
 * %c  (char)               reads a char
 * %d  (int)                reads a signed int
 * %u  (unsigned)           reads an unsigned int
 * %I  (int64_t)            reads a signed int64
 * %U  (uint64_t)           reads an unsigned int64
 *
 * %B  (Buffer* b)          copies the rest into b.buffer by calling b.Write()
 *                          and sets b.length
 * %#B (Buffer* b)          reads a <prefix>:<buffer> into b.length and copies
 *                          the buffer into b.buffer by calling b.Write()
 *
 * %R  (ReadBuffer* b)      points to the rest of the buffer with b.buffer
 *                          and sets b.length
 * %#R (ReadBuffer* b)      reads a <prefix>:<buffer> into b.length and sets
 *                          b.buffer to point to the position
 *
 * Readf returns the number of bytes read from buffer, or -1
 * if the format string did not match the buffer
 */

int Readf(char* buffer, unsigned length, const char* format, ...)
{
    int         read;
    va_list     ap;
    
    va_start(ap, format);
    read = VReadf(buffer, length, format, ap);
    va_end(ap);
    
    return read;
}

int VReadf(char* buffer, unsigned length, const char* format, va_list ap)
{
    char*           c;
    int*            d;
    unsigned*       u;
    int64_t*        i64;
    uint64_t*       u64;
    Buffer*         b;
    ReadBuffer*     rb;
    unsigned        n, l;
    int             read;

#define ADVANCE(f, b)   { format += f; buffer += b; length -= b; read += b; }
#define EXIT()          { return -1; }
#define REQUIRE(r)      { if (length < r) EXIT() }

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
            }
            else if (format[1] == 'c') // %c
            {
                REQUIRE(1);
                c = va_arg(ap, char*);
                *c = buffer[0];
                ADVANCE(2, 1);
            }
            else if (format[1] == 'd') // %d
            {
                d = va_arg(ap, int*);
                *d = BufferToInt64(buffer, length, &n);
                if (n < 1) EXIT();
                ADVANCE(2, n);
            }
            else if (format[1] == 'u') // %u
            {
                u = va_arg(ap, unsigned*);
                *u = BufferToUInt64(buffer, length, &n);
                if (n < 1) EXIT();
                ADVANCE(2, n);
            }
            else if (format[1] == 'I') // %I
            {
                i64 = va_arg(ap, int64_t*);
                *i64 = BufferToInt64(buffer, length, &n);
                if (n < 1) EXIT();
                ADVANCE(2, n);
            }
            else if (format[1] == 'U') // %U
            {
                u64 = va_arg(ap, uint64_t*);
                *u64 = BufferToUInt64(buffer, length, &n);
                if (n < 1) EXIT();
                ADVANCE(2, n);
            }
            else if (format[1] == 'B') // %B no prefix, copies rest
            {
                b = va_arg(ap, Buffer*);
                b->Write(buffer, length);
                ADVANCE(2, b->GetLength());
            }
            else if (length >= 2 && format[1] == '#' && format[2] == 'B') // %#B with prefix, copies
            {
                b = va_arg(ap, Buffer*);
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
                b->Write(buffer, l);
                ADVANCE(3, b->GetLength());
            }
            else if (format[1] == 'R') // %R no prefix, points to rest
            {
                rb = va_arg(ap, ReadBuffer*);
                rb->Wrap(buffer, length);
                ADVANCE(2, rb->GetLength());
            }
            else if (length >= 2 && format[1] == '#' && format[2] == 'R') // %#R with prefix, points
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
                ADVANCE(3, rb->GetLength());
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
 * Writef is a simple snprintf replacement for working with
 * non-null terminated strings
 *
 * supported specifiers:
 * %%                       prints the '%' character
 * %c  (char)               prints a char
 * %d  (int)                prints a signed int
 * %u  (unsigned)           prints an unsigned int
 * %I  (int64_t)            prints a signed int64
 * %U  (uint64_t)           prints an unsigned int64
 * %s  (char* p)            copies strlen(p) bytes from p to the output buffer
 *
 * %B  (Buffer* b)          copies b.buffer without prefix
 * %#B (Buffer* b)          copies b.buffer with prefix
 *
 * %R  (ReadBuffer* b)      copies b.buffer without prefix
 * %#R (ReadBuffer* b)      copies b.buffer with prefix
 *
 * Writef does not null-terminate the resulting buffer
 * returns the number of bytes required or written, or -1 on error
 * (if size bytes were not enough, Writef writes size bytes and returns the
 * number of bytes that would have been required)
 */

int Writef(char* buffer, unsigned size, const char* format, ...)
{
    int         required;
    va_list     ap; 
    
    va_start(ap, format);
    required = VWritef(buffer, size, format, ap);
    va_end(ap);
    
    return required;
}

int VWritef(char* buffer, unsigned size, const char* format, va_list ap)
{
    char            c;
    int             d;
    unsigned        u;
    int             n;
    int64_t         i64;
    uint64_t        u64;
    char*           p;
    unsigned        l, length;
    Buffer*         b;
    ReadBuffer*     rb;
    int             required;
    char            local[64];
    bool            ghost;

#define ADVANCE(f, b)   { format += f; length -= f; if (!ghost) { buffer += b; size -= b; } }
#define EXIT()          { return -1; }
#define REQUIRE(r)      { required += r; if (size < (unsigned)r) ghost = true; }

    ghost = false;
    required = 0;
    length = strlen(format);
    
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
            }
            else if (format[1] == 'c') // %c
            {
                REQUIRE(1);
                c = va_arg(ap, int);
                if (!ghost) buffer[0] = c;
                ADVANCE(2, 1);
            }
            else if (format[1] == 'd') // %d
            {
                d = va_arg(ap, int);
                //n = snprintf(local, sizeof(local), "%d", d);
                n = IntToBuffer(local, sizeof(local), d);
                if (n < 0) EXIT();
                REQUIRE(n);
                if (ghost) n = size;
                memcpy(buffer, local, n);
                ADVANCE(2, n);
            }
            else if (format[1] == 'u') // %u
            {
                u = va_arg(ap, unsigned);
                //n = snprintf(local, sizeof(local), "%u", u);
                n = UIntToBuffer(local, sizeof(local), u);
                if (n < 0) EXIT();
                REQUIRE(n);
                if (ghost) n = size;
                memcpy(buffer, local, n);
                ADVANCE(2, n);
            }
            else if (format[1] == 'I') // %I to print an int64_t 
            {
                i64 = va_arg(ap, int64_t);
                //n = snprintf(local, sizeof(local), "%" PRIi64 "", i64);
                n = Int64ToBuffer(local, sizeof(local), i64);
                if (n < 0) EXIT();
                REQUIRE(n);
                if (ghost) n = size;
                memcpy(buffer, local, n);
                ADVANCE(2, n);
            }
            else if (format[1] == 'U') // %U tp print an uint64_t
            {
                u64 = va_arg(ap, uint64_t);
                //n = snprintf(local, sizeof(local), "%" PRIu64 "", u64);
                n = UInt64ToBuffer(local, sizeof(local), u64);
                if (n < 0) EXIT();
                REQUIRE(n);
                if (ghost) n = size;
                memcpy(buffer, local, n);
                ADVANCE(2, n);
            }
            else if (format[1] == 's') // %s to print a string
            {
                p = va_arg(ap, char*);
                l = strlen(p);
                REQUIRE(l);
                if (ghost) l = size;
                memcpy(buffer, p, l);
                ADVANCE(2, l);
            }
            else if (format[1] == 'B') // %B
            {
                b = va_arg(ap, Buffer*);
                p = b->GetBuffer();
                l = b->GetLength();
                REQUIRE(l);
                if (ghost) l = size;
                memcpy(buffer, p, l);
                ADVANCE(2, l);
            }
            else if (length >= 3 && format[1] == '#' && format[2] == 'B') // %#B
            {
                b = va_arg(ap, Buffer*);
                //n = snprintf(local, sizeof(local), "%u:", b->GetLength());
                n = UIntToBuffer(local, sizeof(local), b->GetLength());
                if (n < 0) EXIT();
                local[n] = ':';
                n += 1;
                REQUIRE(n);
                if (ghost) n = size;
                memcpy(buffer, local, n);
                ADVANCE(0, n);
                REQUIRE(b->GetLength());
                l = b->GetLength();
                if (ghost) l = size;
                memmove(buffer, b->GetBuffer(), l);
                ADVANCE(3, l);
            }
            else if (format[1] == 'R') // %R
            {
                rb = va_arg(ap, ReadBuffer*);
                p = rb->GetBuffer();
                l = rb->GetLength();
                REQUIRE(l);
                if (ghost) l = size;
                memcpy(buffer, p, l);
                ADVANCE(2, l);
            }
            else if (length >= 3 && format[1] == '#' && format[2] == 'R') // %#R
            {
                rb = va_arg(ap, ReadBuffer*);
                n = snprintf(local, sizeof(local), "%u:", rb->GetLength());
                if (n < 0) EXIT();
                REQUIRE(n);
                if (ghost) n = size;
                memcpy(buffer, local, n);
                ADVANCE(0, n);
                REQUIRE(rb->GetLength());
                l = rb->GetLength();
                if (ghost) l = size;
                memmove(buffer, rb->GetBuffer(), l);
                ADVANCE(3, l);
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
