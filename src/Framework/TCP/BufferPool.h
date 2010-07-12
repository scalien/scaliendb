#ifndef BUFFERQUEUE_H
#define BUFFERQUEUE_H

#include "System/Buffers/DynArray.h"

#define	DEFAULT_BUFFERPOOL (BufferPool::Get())

/*
===============================================================================

 BufferPool

 The default implementation just calls new, Allocate() and delete.
===============================================================================
*/

class BufferPool
{
protected:
	typedef DynArray<> Buffer;

public:
	virtual ~BufferPool() {}

	static BufferPool*			Get();

	virtual Buffer*				Acquire(unsigned size);
	virtual void				Release(Buffer* buffer);
};

#endif
