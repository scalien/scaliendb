#include "BufferPool.h"

static BufferPool* bufferPool = NULL;

BufferPool* BufferPool::Get()
{
    if (bufferPool == NULL)
        bufferPool = new BufferPool;
    
    return bufferPool;
}

void BufferPool::Shutdown()
{
    available.DeleteList();
    delete bufferPool;
    bufferPool = NULL;
}

BufferPool::BufferPool()
{
    availableSize = 0;
}

Buffer* BufferPool::Acquire(unsigned size)
{
    Buffer *it;
    
    for (it = available.First(); it != NULL; it = available.Next(it))
    {
        if (it->GetSize() >= size)
        {
            available.Remove(it);
            assert(availableSize - it->GetSize() <= availableSize);
            availableSize -= it->GetSize();
            return it;  
        }
    }
    
    if (available.First() != NULL)
    {
        it = available.First();
        available.Remove(it);
        assert(availableSize - it->GetSize() <= availableSize);
        availableSize -= it->GetSize();
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

Buffer* BufferPool::Allocate(unsigned size)
{
    Buffer* buffer;
    
    buffer = new Buffer();
    buffer->Allocate(size);
    
    return buffer;
}
