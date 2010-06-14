#ifndef STATICARRAY_H
#define STATICARRAY_H

#include "ByteWrap.h"
#include "System/Common.h"

/*
 * StaticArray is a char[n] with pre-allocated space and a length
 * it has a char buffer[n] member and a length member
 */
 
template<int n = 1024>
class StaticArray : public ByteBuffer
{
public:
	StaticArray();

	ByteWrap		ToByteWrap();
	ByteWrap		WrapRest();

	virtual void	Allocate(unsigned size_, bool keepold = true);

private:
	char		data[n];
};

/****************************************************************************/

template<int n>
StaticArray<n>::StaticArray()
{
	buffer = data;
	size = n;
	length = 0;
}

template<int n>
ByteWrap StaticArray<n>::ToByteWrap()
{
	return ByteWrap(buffer, size, length);
}

template<int n>
ByteWrap StaticArray<n>::WrapRest()	
{
	return ByteWrap(buffer + length, size - length, 0);
}

template<int n>
void StaticArray<n>::Allocate(unsigned /*size_*/, bool /*keepold = true*/)
{
	ASSERT_FAIL();
}
 

#endif
