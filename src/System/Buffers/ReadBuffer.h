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
	
	static bool			LessThan(ReadBuffer& a, ReadBuffer& b);
	
private:
	char*				buffer;
	unsigned			length;
};


inline bool ReadBuffer::LessThan(ReadBuffer& a, ReadBuffer& b)
{
	unsigned i;
	unsigned min, alen, blen;
	char *ap, *bp;
	
	ap = a.GetBuffer();
	bp = b.GetBuffer();
	alen = a.GetLength();
	blen = b.GetLength();
	
	min = MIN(alen, blen);
	for (i = 0; i < min; i++)
	{
		if (ap[i] < bp[i])
			return true;
		else if (ap[i] > bp[i])
			return false;
	}
	
	if (alen < blen)
		return true;
	else
		return false;

//	i = memcmp(a.GetBuffer(), b.GetBuffer(), MIN(alen, blen));
//	if (i < 0)
//		return true;
//	else if (i > 0)
//		return false;
//	else
//	{
//		if (alen < blen)
//			return true;
//		else
//			return false;
//	}
}

#endif
