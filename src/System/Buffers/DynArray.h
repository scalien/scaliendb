#ifndef DYNARRAY_H
#define DYNARRAY_H

#include "StaticArray.h"
#include "HeapArray.h"

/*
 * DynArray is a StaticArray up to n bytes, then it becomes a HeapArray
 * with a granularity of 32 bytes
 */

template<int n = 1024>
class DynArray : public ByteBuffer
{
public:
	ByteWrap ToByteWrap();
	ByteWrap WrapRest();

	virtual void Allocate(unsigned size_, bool keepold);
	
	virtual unsigned	Writef(const char* fmt, ...);
	virtual unsigned	Appendf(const char* fmt, ...);

	virtual unsigned	Size() const;
	virtual char*		Buffer() const;
	virtual unsigned	Length() const;
	virtual unsigned	Remaining() const;
	virtual char*		Position() const;
	virtual void		Rewind();
	virtual void		SetLength(unsigned length_);
	
	DynArray*			next;
	
private:
	StaticArray<n>		sa;
	HeapArray			ha;
	
	static const unsigned GRAN = 32;
};

/****************************************************************************/

template<int n>
ByteWrap DynArray<n>::ToByteWrap()
{
	if (ha.Buffer())
		return ha;
	else return sa;
}

template<int n>
ByteWrap DynArray<n>::WrapRest()	
{
	if (ha.Buffer()) 
		return ha.WrapRest();
	else
		return sa.WrapRest();
}

template<int n>
void DynArray<n>::Allocate(unsigned size_, bool keepold)
{
	if (size_ < Size())
		return;
	
	size_ = size_ + GRAN - 1;
	size_ -= size_ % GRAN;

	if (!ha.Buffer() && keepold)
	{
		ha.Allocate(size_, false);
		ha.Write(sa.ToByteString());
	}
	else
		ha.Allocate(size_, keepold);
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

template<int n>
unsigned DynArray<n>::Size() const
 {
	if (ha.Buffer())
		return ha.Size();
	else
		return sa.Size();
}

template<int n>
char* DynArray<n>::Buffer() const
 {
	if (ha.Buffer())
		return ha.Buffer();
	else
		return sa.Buffer();
}

template<int n>
unsigned DynArray<n>::Length() const
{
	if (ha.Buffer())
		return ha.Length();
	else
		return sa.Length();
}

template<int n>
unsigned DynArray<n>::Remaining() const
{
	if (ha.Buffer())
		return ha.Remaining();
	else
		return sa.Remaining();
}

template<int n>
char* DynArray<n>::Position() const
{
	if (ha.Buffer())
		return ha.Position();
	else
		return sa.Position();
}

template<int n>
void DynArray<n>::Rewind()
{
	if (ha.Buffer())
		ha.Rewind();
	else
		sa.Rewind();
}

template<int n>
void DynArray<n>::SetLength(unsigned length_)
{
	if (ha.Buffer())
		ha.SetLength(length_);
	else
		sa.SetLength(length_);
}

#endif
