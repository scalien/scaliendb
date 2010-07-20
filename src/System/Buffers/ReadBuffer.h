#ifndef READBUFFER_H
#define READBUFFER_H

#include "Common.h"

/*
===============================================================================

 ReadBuffer

===============================================================================
*/

class ReadBuffer
{
public:
	ReadBuffer();
	
	void				SetBuffer(char* buffer);
	void				SetLength(unsigned length);
	
	int					Readf(const char* format, ...) const;

	char*				GetBuffer() const;
	unsigned			GetLength() const;
	char				GetCharAt(unsigned i) const;
	uint32_t			GetChecksum() const;
	
	void				Advance(unsigned i);
	
private:
	char*				buffer;
	unsigned			length;
};

#endif
