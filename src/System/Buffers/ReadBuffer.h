#ifndef READBUFFER_H
#define READBUFFER_H

#include "System/Common.h"

class Buffer; // forward

/*
===============================================================================

 ReadBuffer

===============================================================================
*/

class ReadBuffer
{
public:
	ReadBuffer();
	ReadBuffer(char* buffer, unsigned length);
	
	void				Set(char* buffer, unsigned length);
	void				SetBuffer(char* buffer);
	void				SetLength(unsigned length);
	void				Wrap(Buffer& buffer);
	
	int					Readf(const char* format, ...);

	char*				GetBuffer();
	unsigned			GetLength();
	char				GetCharAt(unsigned i);
	uint32_t			GetChecksum();
	
	void				Advance(unsigned i);
	
private:
	char*				buffer;
	unsigned			length;
};

#endif
