#ifndef HEAPARRAY_H
#define HEAPARRAY_H

#include "ByteWrap.h"

/*
 * HeapArray is a buffer allocated on the heap
 */

class HeapArray : public ByteBuffer
{
public:
	HeapArray();
	HeapArray(unsigned size_);
	
	~HeapArray();

	ByteWrap		ToByteWrap();
	ByteWrap		WrapRest();

	virtual void	Allocate(unsigned size_, bool keepold = true);
};

#endif
