#ifndef PAGE_H
#define PAGE_H

#include "System/Buffers/Buffer.h"
#include "System/Buffers/ReadBuffer.h"

/*
===============================================================================

 Page

===============================================================================
*/

class Page
{
public:
	virtual void			Read(ReadBuffer& buffer)	= 0;
	virtual void			Write(Buffer& buffer)		= 0;

	bool					dirty;

	Page*					prev;
	Page*					next;
};

#endif