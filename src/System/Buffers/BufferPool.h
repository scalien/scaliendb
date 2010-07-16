#ifndef BUFFERQUEUE_H
#define BUFFERQUEUE_H

#include "System/Buffers/Buffer.h"
#include "System/Containers/InList.h"

#define	DEFAULT_BUFFERPOOL			(BufferPool::Get())
#define TARGET_AVAILABLE_SIZE		10*MB

/*
===============================================================================

 BufferPool

===============================================================================
*/

class BufferPool
{
public:
	virtual ~BufferPool() {}

	BufferPool();

	static BufferPool*			Get();

	virtual Buffer*				Acquire(unsigned size);
	virtual void				Release(Buffer* buffer);
	
	virtual unsigned			GetAvailableSize();

private:
	InList<Buffer>				available;
	unsigned					availableSize;
	
	Buffer*						Allocate(unsigned size);
};

#endif
