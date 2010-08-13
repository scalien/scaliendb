#include "ReadBuffer.h"
#include "Buffer.h"

ReadBuffer::ReadBuffer()
{
	buffer = NULL;
	length = 0;
}

void ReadBuffer::SetBuffer(char* buffer_)
{
	buffer = buffer_;
}

void ReadBuffer::SetLength(unsigned length_)
{
	length = length_;
}

void ReadBuffer::Wrap(Buffer& buffer_)
{
	buffer = buffer_.GetBuffer();
	length = buffer_.GetLength();
}

int ReadBuffer::Readf(const char* format, ...)
{
	int			read;
	va_list		ap;
	
	va_start(ap, format);
	read = VReadf(buffer, length, format, ap);
	va_end(ap);
	
	return read;
}

char* ReadBuffer::GetBuffer()
{
	return buffer;
}

unsigned ReadBuffer::GetLength()
{
	return length;
}

char ReadBuffer::GetCharAt(unsigned i)
{
	if (i > length - 1)
		ASSERT_FAIL();
	
	return *(buffer + i);
}

uint32_t ReadBuffer::GetChecksum()
{
	return ChecksumBuffer(buffer, length);
}

void ReadBuffer::Advance(unsigned i)
{
	buffer += i;
	length -= i;
	
	assert(length >= 0);
}