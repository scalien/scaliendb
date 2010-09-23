#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "IMF.h"

#define CR                  13
#define LF                  10
#define CS_CR               "\015"
#define CS_LF               "\012"
#define CS_CRLF             CS_CR CS_LF

#ifdef WIN32
#define strncasecmp _strnicmp
#endif

// The return values for Skip and Seek functions are the following:
//
// -1   error
// 0    not enough data or not found
// else the position after the operation
static int SkipWhitespace(char* start, int len)
{
    char *p;
    
    p = start;
    
    if (!p)
        return -1;

    if (len <= 0)
        return -1;

    while (p && *p && *p <= ' ')
    {
        p++;

        if (p - start == len)
            return 0;
    }
    
    if (!*p)
        return -1;
    
    return p - start;   
}

static int SeekWhitespace(char* start, int len)
{
    char *p;
    
    p = start;
    
    if (!p)
        return -1;
    
    if (len <= 0)
        return -1;
    
    while (p && *p && *p != ' ')
    {
        p++;

        if (p - start == len)
            return 0;

    }
    
    if (!*p)
        return -1;
    
    return p - start;
}

static int SkipCrlf(char* start, int len)
{
    char*   p;
    
    p = start;
    
    if (!p)
        return -1;
    
    if (len < 0)
        return -1;
        
    if (len < 2)
        return 0;
    
    while (*p && (*p == CS_CR[0] || *p == CS_LF[0]))
    {
        p++;

        if (p - start == len)
            return 0;

    }
    
    if (!*p)
        return -1;
    
    return p - start;
}

static int SeekCrlf(char* start, int len)
{
    char*   p;
    
    p = start;
    
    if (!p)
        return -1;
    
    if (len < 0)
        return -1;
    
    if (len < 2)
        return 0;
    
    while (*p && (p[0] != CS_CR[0] && p[1] != CS_LF[0]))
    {
        p++;

        if (p - start == len)
            return 0;
    }
    
    if (!*p)
        return -1;
    
    return p - start;   
}

static int SeekChar(char* start, int len, char c)
{
    char*   p;
    
    p = start;
    
    if (!p)
        return -1;
    
    if (len < 0)
        return -1;
    
    if (len < 1)
        return 0;
    
    while (*p && *p != c)
    {
        p++;

        if (p - start == len)
            return 0;
        
    }
    
    if (!*p)
        return -1;
    
    return p - start;
}

static int LineParse(char* buf, int len, ReadBuffer* buffers[3])
{
    char*   p;
    int     pos;

#define remlen (len - (p - buf))
    // p is set so that in remlen it won't be uninitialized
    p = buf;
    pos = 0;
    
    p += pos;
    pos = SkipWhitespace(p, remlen);
    if (pos < 0)
        return pos;
    
    p += pos;
    pos = SeekWhitespace(p, remlen);
    if (pos <= 0) 
        return pos;
    
    buffers[0]->Set(p, pos);

    p += pos;
    pos = SkipWhitespace(p, remlen);
    if (pos <= 0)
        return pos;
    
    p += pos;
    pos = SeekWhitespace(p, remlen);
    if (pos <= 0)
        return pos;

    buffers[1]->Set(p, pos);

    p += pos;
    pos = SkipWhitespace(p, remlen);
    if (pos <= 0) 
        return pos;
    
    p += pos;
    pos = SeekCrlf(p, remlen);
    if (pos <= 0)
        return pos;

    buffers[2]->Set(p, pos);

    p += pos;       
    return (int) (p - buf); 

#undef remlen
}

int IMFHeader::RequestLine::Parse(char* buf, int len, int offs)
{
    ReadBuffer* buffers[3];
    
    buffers[0] = &method;
    buffers[1] = &uri;
    buffers[2] = &version;
    
    return LineParse(buf + offs, len, buffers);
}

int IMFHeader::StatusLine::Parse(char* buf, int len, int offs)
{
    ReadBuffer* buffers[3];
    
    buffers[0] = &version;
    buffers[1] = &code;
    buffers[2] = &reason;
        
    return LineParse(buf + offs, len, buffers);
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
    int pos;

// macro for calculating remaining length
#define remlen ((int) (len - (p - buf)))

    data = buf;
    
    p = buf + offs;
    pos = SkipCrlf(p, remlen);
    if (pos <= 0)
        return pos;
    
    p += pos;
    
    while (p < buf + len) {
        if (remlen == 2 && p[0] == CR && p[1] == LF)
            break;
        
        key = p;
        pos = SeekChar(p, remlen, ':');
        
        if (pos <= 0)
            return pos;

        keylen = pos;
        p += pos + 1;   // skip colon
        
        pos = SkipWhitespace(p, remlen);
        if (pos <= 0)
            return pos;
        
        p += pos;
        value = p;
        pos = SeekCrlf(p, remlen);
        if (pos <= 0)
            return pos;
        
        keyvalue = GetKeyValues(nkv);
        
        keyvalue[nkv].keyStart = (int) (key - buf);
        keyvalue[nkv].keyLength = keylen;
        keyvalue[nkv].valueStart = (int) (value - buf);
        keyvalue[nkv].valueLength = pos;
        nkv++;

        p += pos + 2;   // skip CRLF
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
