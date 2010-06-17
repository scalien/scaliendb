#ifndef BYTESTRING_H
#define BYTESTRING_H

#include "System/Platform.h"

/* 
 * ByteString is like a char* for non-delimited strings
 * it has a 'buffer' member and a 'length' member
 */

class ByteString
{
public:	
	ByteString();
	ByteString(const char* buffer_, unsigned length_);
	ByteString(const char* str);

	ByteString& operator=(const ByteString &other);
	bool operator==(const ByteString& other) const;
	bool operator!=(const ByteString& other) const;

	void			Init();
	void			Clear();

	static bool		Cmp(ByteString& a, ByteString& b);
	bool			Cmp(const char* buffer_, unsigned length_);
	bool			Cmp(const char* str);

	int				Readf(const char* format, ...) const;

	void			Set(const char* str);
	void			Set(const void* buffer_, unsigned length_);

	void			Advance(unsigned n);
	
	char*			Buffer() const;
	unsigned		Length() const;
	char			CharAt(unsigned i) const;
	uint32_t		Checksum();

	void			SetBuffer(char* buffer_);
	void			SetLength(unsigned length_);
	
protected:
	char*			buffer;
	unsigned		length;
};

#endif
