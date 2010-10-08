#ifndef READBUFFER_H
#define READBUFFER_H

#include "System/Common.h"

class Buffer; // forward

/*
===============================================================================

 ReadBuffer

===============================================================================
*/

class ReadBuffer
{
public:
    ReadBuffer();
    ReadBuffer(char* buffer, unsigned length);
    ReadBuffer(const char* s);
    ReadBuffer(Buffer& buffer);
    
    void                SetBuffer(char* buffer);
    void                SetLength(unsigned length);
    void                Wrap(const char* buffer);
    void                Wrap(char* buffer, unsigned length);
    void                Wrap(Buffer& buffer);
    
    int                 Readf(const char* format, ...);

    char*               GetBuffer() const;
    unsigned            GetLength() const;
    char                GetCharAt(unsigned i);
    uint32_t            GetChecksum();
    
    void                Advance(unsigned i);
    
    bool                BeginsWith(const char* s);
    
    static bool         LessThan(const ReadBuffer& a, const ReadBuffer& b);
    static int          Cmp(const ReadBuffer& a, const ReadBuffer& b);
    
private:
    char*               buffer;
    unsigned            length;
};


inline bool ReadBuffer::LessThan(const ReadBuffer& a, const ReadBuffer& b)
{
    return Cmp(a, b) < 0 ? true : false;
}

inline int ReadBuffer::Cmp(const ReadBuffer& a, const ReadBuffer& b)
{
    int ret;
    unsigned alen, blen;

    alen = a.GetLength();
    blen = b.GetLength();
    ret = memcmp(a.GetBuffer(), b.GetBuffer(), MIN(alen, blen));
    
    if (ret != 0)
        return ret;
        
    if (alen < blen)
        return -1;
    else if (blen < alen)
        return 1;
    else
        return 0;
}

#endif
