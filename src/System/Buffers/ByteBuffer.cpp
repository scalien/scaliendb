#include "ByteBuffer.h"

ByteBuffer::ByteBuffer()
{
	Init();
}

ByteString ByteBuffer::ToByteString()
{
	return ByteString(Buffer(), Length());
}

bool ByteBuffer::Cmp(ByteString& a, ByteString& b)
{
	return MEMCMP(a.Buffer(), a.Length(), b.Buffer(), b.Length());
}

bool ByteBuffer::Cmp(const char* buffer_, unsigned length_)
{
	return MEMCMP(buffer, length, buffer_, length_);
}

bool ByteBuffer::Cmp(const char* str)
{
	return MEMCMP(buffer, length, str, strlen(str));
}

void ByteBuffer::Lengthen(unsigned k)
{
	SetLength(Length() + k);
}

unsigned ByteBuffer::Writef(const char* fmt, ...)
{
	va_list ap;
	
	va_start(ap, fmt);
	length = vsnwritef(buffer, size, fmt, ap);
	va_end(ap);	
	
	// snwritef returns number of bytes required
	if (length > size)
		length = 0;
	
	return length;
}

unsigned ByteBuffer::Appendf(const char* fmt, ...)
{
	unsigned required;
	va_list ap;
	
	va_start(ap, fmt);
	required = vsnwritef(buffer + length, Remaining(), fmt, ap);
	va_end(ap);	
	
	// snwritef returns number of bytes required
	if (required > Remaining())
		return 0;
	
	length += required;
	return length;
}

void ByteBuffer::Write(const char* buffer_, unsigned length_)
{
	if (length_ > Size())
		Allocate(length_);
	memcpy(Buffer(), buffer_, length_);
}

void ByteBuffer::Write(const char* str)	
{
	Write(str, strlen(str));
}

void ByteBuffer::Write(ByteString bs)
{
	Write(bs.Buffer(), bs.Length());
}

void ByteBuffer::Append(const char* buffer_, unsigned length_)
{
	if (length_ > Remaining())
		Allocate(length_);
	memcpy(Position(), buffer_, length_);
}

void ByteBuffer::Append(const char* str)
{
	Append(str, strlen(str));
}

void ByteBuffer::Append(ByteString bs)
{
	Append(bs.Buffer(), bs.Length());
}

void ByteBuffer::SetLength(unsigned length_)
{
	length = length_;
	if (length > size)
		ASSERT_FAIL();
}

void ByteBuffer::Init()
{
	buffer = NULL;
	size = 0;
	length = 0;
}

unsigned ByteBuffer::Size() const
{
	return size;
}

char* ByteBuffer::Buffer() const
{
	return buffer;
}

unsigned ByteBuffer::Length() const
{
	return length;
}

unsigned ByteBuffer::Remaining() const
{
	return size - length;
}

char* ByteBuffer::Position() const
{
	return buffer + length;
}

char ByteBuffer::CharAt(unsigned i) const
{
	if (i > Length() - 1)
		ASSERT_FAIL();
	
	return *(buffer + i);
}

uint32_t ByteBuffer::Checksum()
{
	return checksum(buffer, length);
}

void ByteBuffer::Rewind()
{
	length = 0;
}
