#include "Buffer.h"
#include "System/Common.h"
#include <stdarg.h>
#include <stdlib.h>

Buffer::Buffer()
{
    Init();
}

Buffer::~Buffer()
{
    if (buffer != array && !preallocated)
        free(buffer);
}

void Buffer::SetPreallocated(char* buffer_, unsigned size_)
{
    preallocated = true;
    buffer = buffer_;
    size = size_;
}

bool Buffer::Cmp(Buffer& a, Buffer& b)
{
    return MEMCMP(a.buffer, a.length, b.buffer, b.length);
}

bool Buffer::Cmp(const char* buffer_, unsigned length_)
{
    return MEMCMP(buffer, length, buffer_, length_);
}

bool Buffer::Cmp(const char* str)
{
    return MEMCMP(buffer, length, str, strlen(str));
}

void Buffer::Lengthen(unsigned k)
{
    length += k;
}

void Buffer::Shorten(unsigned k)
{
    length -= k;
}

void Buffer::Allocate(unsigned size_, bool keepold)
{
    unsigned    i, newSize;
    char*       newbuffer;
    
    if (size_ <= size)
        return;
    
    //size_ = size_ + ALLOC_GRANURALITY - 1;
    //size_ -= size_ % ALLOC_GRANURALITY;

    newSize = 1;
    for (i = 0; i < 256; i++)
    {
        newSize = newSize * 2;
        if (newSize > size_)
            break;
    }
    if (newSize < size_)
        newSize = size_;
    size_ = newSize;

    if (buffer == array || preallocated)
        newbuffer = (char*) malloc(size_);
    else    
        newbuffer = (char*) realloc(buffer, size_);

    assert(newbuffer != NULL);

    if (keepold && length > 0)
    {
        if (buffer == array)
            memcpy(newbuffer, buffer, length);
    }
    
    buffer = newbuffer;
    size = size_;
    preallocated = false;
}

int Buffer::Readf(const char* format, ...) const
{
    int         read;
    va_list     ap;
    
    va_start(ap, format);
    read = VReadf(buffer, length, format, ap);
    va_end(ap);
    
    return read;
}

unsigned Buffer::Writef(const char* fmt, ...)
{
    unsigned required;
    va_list ap;

    while (true)
    {
        va_start(ap, fmt);
        required = VWritef(buffer, size, fmt, ap);
        va_end(ap);
        
        if (required <= size)
        {
            length = required;
            break;
        }
        
        Allocate(required, false);
    }
    
    return length;
}

unsigned Buffer::Appendf(const char* fmt, ...)
{
    unsigned required;
    va_list ap;
    
    while (true)
    {
        va_start(ap, fmt);
        required = VWritef(GetPosition(), GetRemaining(), fmt, ap);
        va_end(ap);
        
        // snwritef returns number of bytes required
        if (required <= GetRemaining())
            break;
        
        Allocate(length + required, true);
    }
    
    length += required;
    return required;
}

void Buffer::Write(const char* buffer_, unsigned length_)
{
    if (length_ > size)
        Allocate(length_);
    memmove(buffer, buffer_, length_);
    length = length_;
}

void Buffer::Write(const char* str) 
{
    Write(str, strlen(str));
}

void Buffer::Write(Buffer& other)
{
    Write(other.GetBuffer(), other.GetLength());
}

void Buffer::Write(ReadBuffer& other)
{
    Write(other.GetBuffer(), other.GetLength());
}

void Buffer::Append(char c)
{
    Append(&c, 1);
}

void Buffer::Append(const char* buffer_, unsigned length_)
{
    if (length_ > GetRemaining())
        Allocate(length + length_);
    memcpy(GetPosition(), buffer_, length_);
    Lengthen(length_);
}

void Buffer::Append(const char* str)
{
    Append(str, strlen(str));
}

void Buffer::Append(Buffer& other)
{
    Append(other.GetBuffer(), other.GetLength());
}

void Buffer::Append(ReadBuffer other)
{
    Append(other.GetBuffer(), other.GetLength());
}

void Buffer::AppendLittle16(uint16_t x)
{
    x = ToLittle16(x);
    Append((const char*) &x, sizeof(uint16_t));
}

void Buffer::AppendLittle32(uint32_t x)
{
    x = ToLittle32(x);
    Append((const char*) &x, sizeof(uint32_t));
}

void Buffer::AppendLittle64(uint64_t x)
{
    x = ToLittle64(x);
    Append((const char*) &x, sizeof(uint64_t));
}

char Buffer::GetCharAt(unsigned i)
{
    if (i > length - 1)
        ASSERT_FAIL();
    
    return *(buffer + i);
}

void Buffer::SetCharAt(unsigned i, char c)
{
    buffer[i] = c;
}

void Buffer::NullTerminate()
{
    Append("", 1);
}

void Buffer::Zero()
{
    memset(buffer, 0, size);
}

void Buffer::ZeroRest()
{
    memset(buffer + length, 0, size - length);
}

void Buffer::SetLength(unsigned length_)
{
    length = length_;
    if (length > size)
        ASSERT_FAIL();
}

void Buffer::Init()
{
    buffer = array;
    size = SIZE(array);
    length = 0;
    preallocated = false;
    prev = next = this;
}

unsigned Buffer::GetSize()
{
    return size;
}

char* Buffer::GetBuffer()
{
    return buffer;
}

unsigned Buffer::GetLength()
{
    return length;
}

unsigned Buffer::GetRemaining()
{
    return size - length;
}

char* Buffer::GetPosition()
{
    return buffer + length;
}

uint32_t Buffer::GetChecksum()
{
    return ChecksumBuffer(buffer, length);
}

void Buffer::Clear()
{
    length = 0;
}

Buffer::Buffer(const Buffer& other)
{
//    ASSERT_FAIL();
    *this = other;  // call operator=()
}

Buffer& Buffer::operator=(const Buffer& other)
{
//    ASSERT_FAIL();
    if (other.size != size)
      Allocate(other.size, false);

    memcpy(buffer, other.buffer, other.size);
    length = other.length;
    prev = next = this;

    return *this;
}
