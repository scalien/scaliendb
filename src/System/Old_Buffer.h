#ifndef BUFFER_H
#define BUFFER_H

#if 0

#include "Common.h"
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>

class ByteString
{
public:
	unsigned	size;
	unsigned	length;
	char*		buffer;
	
	ByteString() { Init(); }
	
	ByteString(int size_, int length_, const void* buffer_)
		: size(size_), length(length_), buffer((char*) buffer_) {}

	ByteString(const char* str)
	{
		buffer = (char*) str;
		size = strlen(str);
		length = size;
	}
	
	virtual ~ByteString() {}
	
	virtual void Init() { size = 0; length = 0; buffer = 0; }

	
	virtual bool Set(const char* str)
	{
		if (buffer == NULL)
			ASSERT_FAIL();
		
		int len = strlen(str);
		return Set(str, len);
	}
	
	virtual bool Set(const void* buf, unsigned len)
	{
		if (len > 0 && (buf == NULL || buffer == NULL))
			ASSERT_FAIL();
		
		if (len > size)
			return false;
		
		memmove(buffer, buf, len);
		
		length = len;
		
		return true;
	}
	
	virtual bool Set(const ByteString& other)
	{
		buffer = other.buffer;
		length = other.length;
		size = other.size;
		
		return true;
	}

	bool operator==(const ByteString& other) const
	{
		return MEMCMP(buffer, length, other.buffer, other.length);
	}

	bool operator!=(const ByteString& other) const
	{
		return !(operator==(other));
	}
	
	bool Advance(unsigned n)
	{
		if (length < n)
			return false;
		
		buffer += n;
		length -= n;
		size   -= n;
		
		return true;
	}
	
	void Clear() { length = 0; }
	
	virtual bool Writef(const char* fmt, ...)
	{
		va_list ap;
		
		va_start(ap, fmt);
		length = vsnwritef(buffer, size, fmt, ap);
		va_end(ap);
		
		if (length > size)
		{
			length = size;
			return false;
		}
		
		return true;
	}
	
	int Remaining() const
	{
		return size - length;
	}

	ByteString& operator=(const ByteString &bs)
	{
		Set(bs);
		return *this;
	}
};

class ByteBuffer : public ByteString
{
public:
	ByteBuffer()
	{
		buffer = NULL;
		size = 0;
		length = 0;
	}
	
	~ByteBuffer()
	{
		Free();
	}
	
	virtual void Init()
	{
		length = 0;
	}
	
	virtual bool Set(const char* str)
	{
		if (buffer == NULL)
			ASSERT_FAIL();
		
		int len = strlen(str);
		return Set(str, len);
	}
	
	virtual bool Set(const void* buffer, unsigned len)
	{
		if (!Reallocate(len))
			return false;
		return ByteString::Set(buffer, len);
	}
	
	virtual bool Set(const ByteString& other)
	{
		return Set(other.buffer, other.length);
	}
	
	virtual bool Writef(const char* fmt, ...)
	{
		va_list ap;
		
		va_start(ap, fmt);
		length = vsnwritef(buffer, size, fmt, ap);
		va_end(ap);
		
		if (length < 0)
			return false;
		if (length > size)
		{
			if (!Reallocate(length))
				return false;
		
			va_start(ap, fmt);
			length = vsnwritef(buffer, size, fmt, ap);
			va_end(ap);
		}
		
		return true;
	}
		
	bool Allocate(int size_)
	{
		if (buffer != NULL)
			ASSERT_FAIL();
			
		buffer = (char*) alloc(size_);
		if (buffer == NULL)
			return false;
		size = size_;
		length = 0;
		return true;
	}

	bool Reallocate(unsigned size_)
	{
		if (size_ <= size)
		{
			length = 0;
			return true;
		}
		
		if (buffer != NULL)
			free(buffer);
		
		buffer = (char*) alloc(size_);
		if (buffer == NULL)
			return false;
		size = size_;
		length = 0;
		return true;
	}
	
	void Free()
	{
		if (buffer != NULL)
			free(buffer);
		buffer = NULL;
		size = 0;
		length = 0;
	}
	
private:
	ByteString& operator=(const ByteString&) { return *this; }
};

template<int n>
class ByteArray : public ByteString
{
public:
	char	data[n];
	
	ByteArray() { size = n; length = 0; buffer = data; }
	
	ByteArray(const char* str)
	{
		size = n;
		length = strlen(str);
		memcpy(data, str, length);
		buffer = data;
	}
	
	virtual void Init() { length = 0; }
	
	bool Set(const char* str) { return ByteString::Set(str); }

	bool Set(const char* str, int len) { return ByteString::Set(str, len); }

	bool Set(const ByteString& bs)	{ return ByteString::Set(bs.buffer, bs.length); }
};

template<int n>
class DynArray : public ByteString
{
public:
	char		data[n];
	DynArray*	next;
	
	enum { GRAN = 32 };
	
	DynArray()
	{
		size = n;
		length = 0;
		buffer = data;
	}
	
	~DynArray()
	{
		if (buffer != data)
			delete[] buffer;
	}
	
	void Init()
	{
		length = 0;
	}
	
	bool Set(const ByteString &bs)
	{
		Clear();
		return Append(bs);
	}
	
	bool Set(const void* buf, unsigned len)
	{
		Clear();
		return Append(buf, len);		
	}
	
	bool Append(const ByteString &bs)
	{
		return Append(bs.buffer, bs.length);
	}
	
	bool Append(const void *buf, unsigned len)
	{
		if (length + len > size)
			Reallocate(length + len, true);

		memmove(buffer + length, buf, len);
		length += len;
		
		return true;
	}
	
	void Reallocate(unsigned newsize, bool keepold)
	{
		char *newbuffer;
		
		if (newsize <= size)
			return;
		
		newsize = newsize + GRAN - 1;
		newsize -= newsize % GRAN;
		
		newbuffer = new char[newsize];
		
		if (keepold && length > 0)
			memcpy(newbuffer, buffer, length);

		if (buffer != data)
			delete[] buffer;
		
		buffer = newbuffer;
		size = newsize;
	}

	virtual bool Writef(const char* fmt, ...)
	{
		va_list ap;

		while (true)
		{
			va_start(ap, fmt);
			length = vsnwritef(buffer, size, fmt, ap);
			va_end(ap);
			
			if (length <= size)
				return true;

			Reallocate(length, false);
		}
		
		return true;
	}

	bool Fill(int c, unsigned len)
	{
		Reallocate(len, false);
		memset(buffer, c, len);
		
		return true;
	}

	DynArray<n> & Remove(int start, int count)
	{
		length -= count;
		memmove(buffer + start, buffer + start + count, length - start);

		return *this;
	}
};

#endif

#endif
