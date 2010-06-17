#include "ByteString.h"

#include "System/Common.h"

ByteString::ByteString()
{
	Init();
}

ByteString::ByteString(const char* buffer_, unsigned length_)
{
	Set(buffer_, length_);
}

ByteString::ByteString(const char* str)
{
	Set(str);
}

ByteString& ByteString::operator=(const ByteString &other)
{
	Set(other.buffer, other.length); return *this;
}

bool ByteString::operator==(const ByteString& other) const
{
	return (buffer == other.buffer && length == other.length);
}

bool ByteString::operator!=(const ByteString& other) const
{
	return !(operator==(other));
}

void ByteString::Init()
{
	buffer = 0; length = 0;
}

void ByteString::Clear()
{
	length = 0;
}

bool ByteString::Cmp(ByteString& a, ByteString& b)
{
	return MEMCMP(a.Buffer(), a.Length(), b.Buffer(), b.Length());
}

bool ByteString::Cmp(const char* buffer_, unsigned length_)
{
	return MEMCMP(buffer, length, buffer_, length_);
}

bool ByteString::Cmp(const char* str)
{
	return MEMCMP(buffer, length, str, strlen(str));
}

int ByteString::Readf(const char* format, ...) const
{
	int			read;
	va_list		ap;
	
	va_start(ap, format);
	read = vsnreadf(buffer, length, format, ap);
	va_end(ap);
	
	return read;
}

void ByteString::Set(const char* str)
{
	Set(str, strlen(str));
}

void ByteString::Set(const void* buffer_, unsigned length_)
{
	buffer = (char*) buffer_; length = length_;
}

void ByteString::Advance(unsigned n)
{
	if (length < n)
		ASSERT_FAIL();
	
	buffer += n;
	length -= n;
}

char* ByteString::Buffer() const
{
	return buffer;
}

unsigned ByteString::Length() const
{
	return length;
}

char ByteString::CharAt(unsigned i) const
{
	if (i > Length() - 1)
		ASSERT_FAIL();
	
	return *(buffer + i);
}

uint32_t ByteString::Checksum()
{
	return checksum(buffer, length);
}

void ByteString::SetBuffer(char* buffer_)
{
	buffer = buffer_;
}

void ByteString::SetLength(unsigned length_)
{
	length = length_;
}
