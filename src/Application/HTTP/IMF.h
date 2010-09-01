#ifndef IMF_H
#define IMF_H

#include "System/Buffers/ReadBuffer.h"

/*
===============================================================================

 Internet message format based on RFC 2822.
 Suitable for parsing HTTP, SIP and MIME headers.

===============================================================================
*/

class IMFHeader
{
public:
	class StorageKeyValue
	{
	public:
		int keyStart;
		int keyLength;
		int valueStart;
		int valueLength;
	};

	class RequestLine
	{
	public:
		ReadBuffer	method;
		ReadBuffer	uri;
		ReadBuffer	version;
		
		int Parse(char* buf, int len, int offs);
	};

	class StatusLine
	{
	public:
		ReadBuffer	version;
		ReadBuffer	code;
		ReadBuffer	reason;
		
		int Parse(char* buf, int len, int offs);
	};
	
public:	
	enum { KEYVAL_BUFFER_SIZE = 16 };
	int				numKeyval;
	int				capKeyval;
	StorageKeyValue*		keyvalues;
	StorageKeyValue		keyvalBuffer[KEYVAL_BUFFER_SIZE];
	char*			data;	
	
	IMFHeader();
	~IMFHeader();
	
	void			Init();
	void			Free();
	int				Parse(char* buf, int len, int offs);
	const char*		GetField(const char* key);
	
	StorageKeyValue*		GetStorageKeyValues(int newSize);
};



#endif
