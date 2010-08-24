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
	Page()					{ dirty = false; prev = next = this; }
	virtual void			Read(ReadBuffer& buffer)	= 0;
	virtual void			Write(Buffer& buffer)		= 0;

	void					SetDirty(bool dirty_) { dirty = dirty_; }
	bool					IsDirty() { return dirty; }

	void					SetPageSize(uint32_t pageSize_) { pageSize = pageSize_; }
	uint32_t				GetPageSize() { return pageSize; }

	void					SetOffset(uint32_t offset_) { offset = offset_; }
	uint32_t				GetOffset() { return offset; }

	Page*					prev;
	Page*					next;

protected:

	bool					dirty;
	uint32_t				pageSize;
	uint32_t				offset;
};

#endif