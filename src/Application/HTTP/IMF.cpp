#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "IMF.h"

#define CR					13
#define LF					10
#define CS_CR				"\015"
#define CS_LF				"\012"
#define CS_CRLF				CS_CR CS_LF

#ifdef WIN32
#define strncasecmp _strnicmp
#endif

static char* SkipWhitespace(char* p, int len)
{
	if (!p)
		return NULL;

	if (len <= 0)
		return NULL;

	while (p && *p && *p <= ' ')
	{
		if (--len == 0)
			return NULL;

		p++;
	}
	
	if (!*p)
		return NULL;
	
	return p;	
}

static char* SeekWhitespace(char* p, int len)
{
	if (!p)
		return NULL;
	
	if (len <= 0)
		return NULL;
	
	while (p && *p && *p != ' ')
	{
		if (--len == 0)
			return NULL;

		p++;
	}
	
	if (!*p)
		return NULL;
	
	return p;	
}

static char* SkipCrlf(char* p, int len)
{
	if (!p)
		return NULL;
	
	if (len < 2)
		return NULL;
	
	while (*p && (*p == CS_CR[0] || *p == CS_LF[0]))
	{
		if (--len == 0)
			return NULL;

		p++;
	}
	
	if (!*p)
		return NULL;
	
	return p;	
}

static char* SeekCrlf(char* p, int len)
{
	if (!p)
		return NULL;
	
	if (len < 2)
		return NULL;
	
	while (*p && (p[0] != CS_CR[0] && p[1] != CS_LF[0]))
	{
		if (--len == 0)
			return NULL;

		p++;
	}
	
	if (!*p)
		return NULL;
	
	return p;	
}

static char* SeekChar(char* p, int len, char c)
{
	if (!p)
		return NULL;
	
	if (len < 1)
		return NULL;
	
	while (*p && *p != c)
	{
		if (--len == 0)
			return NULL;
		
		p++;
	}
	
	if (!*p)
		return NULL;
	
	return p;
}

static int LineParse(char* buf, int len, int offs, const char** values[3])
{
	char* p;

#define remlen (len - (p - buf))
	// p is set so that in remlen it won't be uninitialized
	p = buf;	
	p = SkipWhitespace(buf + offs, remlen);
	
	*values[0] = p;
	p = SeekWhitespace(p, remlen);
	if (!p) return -1;
	
	*p++ = '\0';
	p = SkipWhitespace(p, remlen);
	if (!p) return -1;
	
	*values[1] = p;
	p = SeekWhitespace(p, remlen);
	if (!p) return -1;
	
	*p++ = '\0';
	p = SkipWhitespace(p, remlen);
	if (!p) return -1;
	
	*values[2] = p;
	p = SeekCrlf(p, remlen);
	if (!p) return -1;
	
	*p = '\0';
	p += 2;
	
	return (int) (p - buf);	

#undef remlen
}

int IMFHeader::RequestLine::Parse(char* buf, int len, int offs)
{
	const char** values[3];
	
	values[0] = &method;
	values[1] = &uri;
	values[2] = &version;
	
	return LineParse(buf, len, offs, values);
}

int IMFHeader::StatusLine::Parse(char* buf, int len, int offs)
{
	const char** values[3];
	
	values[0] = &version;
	values[1] = &code;
	values[2] = &reason;
	
	return LineParse(buf, len, offs, values);
}

IMFHeader::IMFHeader()
{
	Init();
}

IMFHeader::~IMFHeader()
{
	Free();
}

void IMFHeader::Init()
{
	numKeyval = 0;
	capKeyval = KEYVAL_BUFFER_SIZE;
	keyvalues = keyvalBuffer;
	data = NULL;	
}

void IMFHeader::Free()
{
	if (keyvalues != keyvalBuffer)
		delete[] keyvalues;
	keyvalues = keyvalBuffer;
}

int IMFHeader::Parse(char* buf, int len, int offs)
{
	char* p;
	char* key;
	char* value;
	int nkv = numKeyval;
	KeyValue* keyvalue;
	int keylen;

// macro for calculating remaining length
#define remlen ((int) (len - (p - buf)))

	data = buf;
	
	p = buf + offs;
	p = SkipCrlf(p, remlen);
	if (!p)
		return -1;
	
	while (p < buf + len) {
		key = p;
		p = SeekChar(p, remlen, ':');
		
		if (p)
		{
			keylen = (int) (p - key);
			
			*p++ = '\0';
			p = SkipWhitespace(p, remlen);
			
			value = p;
			p = SeekCrlf(p, remlen);
			if (p)
			{
				keyvalue = GetKeyValues(nkv);
				
				keyvalue[nkv].keyStart = (int) (key - buf);
				keyvalue[nkv].keyLength = keylen;
				keyvalue[nkv].valueStart = (int) (value - buf);
				keyvalue[nkv].valueLength = (int) (p - value);
				nkv++;

				*p = '\0';
				p += 2;
			}
			else
			{
				p = key;
				break;
			}
		}
		else
		{
			p = key;
			break;
		}
	}
	
	numKeyval = nkv;
	
	return (int) (p - buf);

#undef remlen	
}

const char* IMFHeader::GetField(const char* key)
{
	KeyValue* keyval;
	int i;
	
	if (!data)
		return NULL;
	
	i = 0;
	while (i < numKeyval)
	{
		keyval = &keyvalues[i];
		if (strncasecmp(key, data + keyval->keyStart, keyval->keyLength) == 0)
			return data + keyval->valueStart;

		i++;
	}
	
	return NULL;
}


IMFHeader::KeyValue* IMFHeader::GetKeyValues(int newSize)
{
	KeyValue* newkv;
	
	if (newSize < capKeyval)
		return keyvalues;
	
	capKeyval *= 2;
	newkv = new KeyValue[capKeyval];
	memcpy(newkv, keyvalues, numKeyval * sizeof(KeyValue));
	
	if (keyvalues != keyvalBuffer)
		delete[] keyvalues;
	
	keyvalues = newkv;
	
	return keyvalues;
}
