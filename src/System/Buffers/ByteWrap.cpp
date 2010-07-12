#include "ByteWrap.h"

#include "System/Common.h"

ByteWrap::ByteWrap()
{
	Init();
}

ByteWrap::ByteWrap(char* buffer_, unsigned size_)
{
	Wrap(buffer_, size_);
}

ByteWrap::ByteWrap(char* buffer_, unsigned size_, unsigned length_)
{
	buffer = buffer_;
	size = size_;	
	length = length_;
}

void ByteWrap::Init()
{
	Wrap(NULL, 0);
}

void ByteWrap::Wrap(char* buffer_, unsigned size_)
{
	buffer = buffer_;
	size = size_;
	length = 0;
}

void ByteWrap::Wrap(ByteBuffer& buffer)
{
	Wrap(buffer.Buffer(), buffer.Size());
}

void ByteWrap::Wrap(ByteString& bs)
{
	Wrap(bs.Buffer(), bs.Length());
}

void ByteWrap::Allocate(unsigned /*size_*/, bool /*keepold = true*/)
{
	ASSERT_FAIL();
}
 