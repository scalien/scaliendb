#include "HeapArray.h"
#include "System/Common.h"

HeapArray::HeapArray()
{
	Init();
}

HeapArray::HeapArray(unsigned size_)
{
	Init();
	if (!(buffer = (char*)malloc(size_)))
		ASSERT_FAIL(); size = size_;
}

HeapArray::~HeapArray()
{
	if (buffer)
		free(buffer);
}

ByteWrap HeapArray::ToByteWrap()
{
	return ByteWrap(buffer, size, length);
}

ByteWrap HeapArray::WrapRest()	
{
	return ByteWrap(buffer + length, size - length, 0);
}

void HeapArray::Allocate(unsigned size_, bool keepold)
{
	char* newbuffer;
	
	if (size_ <= size)
	{
		if (!keepold)
			length = 0;
		return;
	}

	newbuffer = (char*) alloc(size_);
	if (newbuffer == NULL)
		ASSERT_FAIL();
	
	if (buffer != NULL)
	{
		if (keepold)
			memcpy(newbuffer, buffer, length);
		free(buffer);
	}
	
	buffer = newbuffer;
	size = size_;
}
