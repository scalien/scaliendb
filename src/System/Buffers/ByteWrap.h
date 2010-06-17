#ifndef BYTEWRAP_H
#define BYTEWRAP_H

#include "ByteBuffer.h"

/*
 * ByteWrap is something that wraps a pre-allocated chunk of memory
 * and records the length of whatever is stored in there
 */

class ByteWrap : public ByteBuffer
{
public:
	ByteWrap();
	ByteWrap(char* buffer_, unsigned size_);
	ByteWrap(char* buffer_, unsigned size_, unsigned length_);

	virtual void	Init();
	virtual	void	Wrap(ByteBuffer& buffer);
	virtual void	Wrap(char* buffer_, unsigned size_);
	virtual void	Allocate(unsigned size_, bool keepold = true);
};

#endif
