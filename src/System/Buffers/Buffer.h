#ifndef BUFFER_H
#define BUFFER_H

#include "System/Platform.h"
#include "ReadBuffer.h"

#define ARRAY_SIZE				128
#define ALLOC_GRANURALITY		32

#define P(b)					(b)->GetLength(), (b)->GetBuffer()

/*
===============================================================================

 BufferPool

===============================================================================
*/

class Buffer
{
public:
	Buffer();
	
	static bool			Cmp(Buffer& a, Buffer& b);
	bool				Cmp(const char* buffer_, unsigned length_);
	bool				Cmp(const char* str);

	void				Lengthen(unsigned k);
	void				Shorten(unsigned k);
	
	void				Allocate(unsigned size_, bool keepold = true);

	int					Readf(const char* format, ...) const;
	unsigned			Writef(const char* fmt, ...);
	unsigned			Appendf(const char* fmt, ...);
	
	void				Write(const char* buffer_, unsigned length_);
	void				Write(const char* str);
	void				Write(Buffer& b);
	void				Write(ReadBuffer& b);

	void				Append(const char* buffer_, unsigned length_);
	void				Append(const char* str);
	void				Append(Buffer& b);

	void				SetLength(unsigned length_);

	void				Init();
	unsigned			GetSize() const;
	char*				GetBuffer() const;
	unsigned			GetLength() const;
	unsigned			GetRemaining() const;
	char*				GetPosition() const;
	char				GetCharAt(unsigned i) const;
	uint32_t			GetChecksum() const;
	
	virtual void		Rewind();

	Buffer*				next;
	Buffer*				prev;
		
protected:
	char				array[ARRAY_SIZE];
	char*				buffer;
	unsigned			size;
	unsigned			length;
};

#endif
