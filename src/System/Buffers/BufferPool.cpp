#include "BufferPool.h"

static BufferPool* bufferPool = NULL;

BufferPool* BufferPool::Get()
{
	if (bufferPool == NULL)
		bufferPool = new BufferPool;
	
	return bufferPool;
}

BufferPool::BufferPool()
{
	availableSize = 0;
}

Buffer*	BufferPool::Acquire(unsigned size)
{
	Buffer *it;
	
	for (it = available.Head(); it != NULL; it = available.Next(it))
	{
		if (it->GetSize() >= size)
		{
			available.Remove(it);
			availableSize -= it->GetSize();
			assert(size >= 0);
			return it;	
		}
	}
	
	if (available.Head() != NULL)
	{
		it = available.Head();
		available.Remove(it);
		availableSize -= it->GetSize();
		assert(size >= 0);
	}
	else
		it = new Buffer();

	it->Allocate(size);

	return it;
}

void BufferPool::Release(Buffer* buffer)
{
	if (buffer == NULL)
		return;
	
	buffer->Clear();
	
	if (availableSize < TARGET_AVAILABLE_SIZE)
	{
		available.Prepend(buffer);
		availableSize += buffer->GetSize();
	}
	else
		delete buffer;
}

unsigned BufferPool::GetAvailableSize()
{
	return availableSize;
}

Buffer*	BufferPool::Allocate(unsigned size)
{
	Buffer* buffer;
	
	buffer = new Buffer();
	buffer->Allocate(size);
	
	return buffer;
}
