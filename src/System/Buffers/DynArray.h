#ifndef DYNARRAY_H
#define DYNARRAY_H

#include "ByteWrap.h"
#include "System/Common.h"

/*
 * DynArray is a StaticArray up to n bytes, then it becomes a HeapArray
 * with a granularity of 32 bytes
 */

template<int n = 1024>
class DynArray : public ByteBuffer
{
public:
	DynArray();
	~DynArray();
	
	ByteWrap ToByteWrap();
	ByteWrap WrapRest();

	virtual void Allocate(unsigned size_, bool keepold = true);
	
	virtual unsigned	Writef(const char* fmt, ...);
	virtual unsigned	Appendf(const char* fmt, ...);

	DynArray*			next;
	
private:
	char				data[n];
	
	static const unsigned GRAN = 32;
};

/****************************************************************************/

template<int n>
DynArray<n>::DynArray()
{
	buffer = data;
}

template<int n>
DynArray<n>::~DynArray()
{
	if (buffer != data)
		delete[] buffer;
}

template<int n>
ByteWrap DynArray<n>::ToByteWrap()
{
	return ByteWrap(this);
}

template<int n>
ByteWrap DynArray<n>::WrapRest()	
{
	return ByteWrap(Position(), Remaining());
}

template<int n>
void DynArray<n>::Allocate(unsigned size_, bool keepold)
{
	char* newbuffer;
	
	if (size_ < Size())
		return;
	
	size_ = size_ + GRAN - 1;
	size_ -= size_ % GRAN;

	newbuffer = new char[size_];
	if (keepold && length > 0)
		memcpy(newbuffer, buffer, length);
	
	if (buffer != data)
		delete[] buffer;
		
	buffer = newbuffer;
	size = size_;
}

template<int n>
unsigned DynArray<n>::Writef(const char* fmt, ...)
{
	unsigned required;
	va_list ap;

	while (true)
	{
		va_start(ap, fmt);
		required = vsnwritef(Buffer(), Size(), fmt, ap);
		va_end(ap);
		
		if (required <= Size())
		{
			SetLength(required);
			return Length();
		}
		
		Allocate(required, false);
	}
	
	return Length();
}

template<int n>
unsigned DynArray<n>::Appendf(const char* fmt, ...)
{
	unsigned required;
	va_list ap;
	
	while (true)
	{
		va_start(ap, fmt);
		required = vsnwritef(Buffer() + Length(), Remaining(), fmt, ap);
		va_end(ap);
		
		// snwritef returns number of bytes required
		if (required <= Remaining())
			break;
		
		Allocate(Length() + required, true);
	}
	
	Lengthen(required);
	return required;
}

#endif
