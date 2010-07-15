#ifndef BUFFERQUEUE_H
#define BUFFERQUEUE_H

#include "System/Buffer/Buffer.h"
#include "System/Containers/ListP.h"

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
	ListP<Buffer>				available;
	unsigned					availableSize;
	
	Buffer*						Allocate(unsigned size);
};

#endif
