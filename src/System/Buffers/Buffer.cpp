#include "Buffer.h"
#include <stdarg.h>
#include "System/Common.h"

Buffer::Buffer()
{
	Init();
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
	char* newbuffer;
	
	if (size_ < size)
		return;
	
	size_ = size_ + ALLOC_GRANURALITY - 1;
	size_ -= size_ % ALLOC_GRANURALITY;

	newbuffer = new char[size_];
	if (keepold && length > 0)
		memcpy(newbuffer, buffer, length);
	
	if (buffer != array)
		delete[] buffer;
		
	buffer = newbuffer;
	size = size_;
}

int Buffer::Readf(const char* format, ...) const
{
	int			read;
	va_list		ap;
	
	va_start(ap, format);
	read = vsnreadf(buffer, length, format, ap);
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
		required = vsnwritef(buffer, size, fmt, ap);
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
		required = vsnwritef(GetPosition(), GetRemaining(), fmt, ap);
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
	memcpy(buffer, buffer_, length_);
	length = length_;
}

void Buffer::Write(const char* str)	
{
	Write(str, strlen(str));
}

void Buffer::Write(const Buffer& b)
{
	Write(b.GetBuffer(), b.GetLength());
}

void Buffer::Write(const ReadBuffer& b)
{
	Write(b.GetBuffer(), b.GetLength());
}

void Buffer::Append(const char* buffer_, unsigned length_)
{
	if (length_ > GetRemaining())
		Allocate(length_);
	memcpy(GetPosition(), buffer_, length_);
	Lengthen(length_);
}

void Buffer::Append(const char* str)
{
	Append(str, strlen(str));
}

void Buffer::Append(Buffer& b)
{
	Append(b.GetBuffer(), b.GetLength());
}

void Buffer::SetLength(unsigned length_)
{
	length = length_;
	if (length < 0 || length > size)
		ASSERT_FAIL();
}

void Buffer::Init()
{
	buffer = array;
	size = SIZE(array);
	length = 0;
}

unsigned Buffer::GetSize() const
{
	return size;
}

char* Buffer::GetBuffer() const
{
	return buffer;
}

unsigned Buffer::GetLength() const
{
	return length;
}

unsigned Buffer::GetRemaining() const
{
	return size - length;
}

char* Buffer::GetPosition() const
{
	return buffer + length;
}

char Buffer::GetCharAt(unsigned i) const
{
	if (i > length - 1)
		ASSERT_FAIL();
	
	return *(buffer + i);
}

uint32_t Buffer::GetChecksum() const
{
	return checksum(buffer, length);
}

void Buffer::Rewind()
{
	length = 0;
}
