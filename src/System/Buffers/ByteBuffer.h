#ifndef BYTEBUFFER_H
#define BYTEBUFFER_H

#include "ByteString.h"

/*
 * ByteBuffer is an abstract base class for byte storage objects which
 * provide read/write access to their buffer
 */

class ByteBuffer
{
public:
	ByteBuffer();
	virtual ~ByteBuffer() {};

	ByteString			ToByteString();

	static bool			Cmp(ByteString& a, ByteString& b);
	bool				Cmp(const char* buffer_, unsigned length_);
	bool				Cmp(const char* str);

	void				Lengthen(unsigned k);
	void				Shorten(unsigned k);
	
	virtual void		Allocate(unsigned size_, bool keepold = true) = 0;

	virtual unsigned	Writef(const char* fmt, ...);
	virtual unsigned	Appendf(const char* fmt, ...);
	
	virtual void		Write(const char* buffer_, unsigned length_);
	virtual void		Write(const char* str);
	virtual void		Write(ByteString bs);

	virtual void		Append(const char* buffer_, unsigned length_);
	virtual void		Append(const char* str);
	virtual void		Append(ByteString bs);

	virtual void		SetLength(unsigned length_);

	virtual void		Init();
	virtual unsigned	Size() const;
	virtual char*		Buffer() const;
	virtual unsigned	Length() const;
	virtual unsigned	Remaining() const;
	virtual char*		Position() const;
	char				CharAt(unsigned i) const;
	uint32_t			Checksum();
	
	virtual void		Rewind();
		
protected:
	char*				buffer;
	unsigned			size;
	unsigned			length;
};

#endif
