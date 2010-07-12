#include "BufferPool.h"

static BufferPool* bufferPool = NULL;

BufferPool* BufferPool::Get()
{
	if (bufferPool == NULL)
		bufferPool = new BufferPool;
	
	return bufferPool;
}

DynArray<>*	BufferPool::Acquire(unsigned size)
{
	Buffer* buffer;
	
	buffer = new Buffer();
	buffer->Allocate(size);
	
	return buffer;
}

void BufferPool::Release(Buffer* buffer)
{
	if (buffer)
		delete buffer;
}
