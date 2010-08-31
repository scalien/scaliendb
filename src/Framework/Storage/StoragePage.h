#ifndef STORAGEPAGE_H
#define STORAGEPAGE_H

#include "System/Buffers/Buffer.h"
#include "System/Buffers/ReadBuffer.h"

/*
===============================================================================

 StoragePage

===============================================================================
*/

class StoragePage
{
public:
	StoragePage();
	virtual ~StoragePage() {}

	void				SetFileIndex(uint32_t fileIndex);
	uint32_t			GetFileIndex();

	void				SetOffset(uint32_t offset);
	uint32_t			GetOffset();

	void				SetPageSize(uint32_t pageSize);
	uint32_t			GetPageSize();

	void				SetDirty(bool dirty);
	bool				IsDirty();
	
	void				SetNew(bool n);
	bool				IsNew();

	virtual void		Read(ReadBuffer& buffer)	= 0;
	virtual void		Write(Buffer& buffer)		= 0;

	StoragePage*		prev;
	StoragePage*		next;

	Buffer				buffer;

protected:

	uint32_t			fileIndex;
	uint32_t			offset;
	uint32_t			pageSize;
	bool				dirty;
	bool				newPage;
};

#endif