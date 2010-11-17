#ifndef BUFFER_H
#define BUFFER_H

#include "System/Platform.h"
#include "ReadBuffer.h"

#define ARRAY_SIZE              128
#define ALLOC_GRANURALITY       32

/*
===============================================================================

 Buffer

===============================================================================
*/

class Buffer
{
public:
    Buffer();
    Buffer(const Buffer& other);
    ~Buffer();

    Buffer&             operator=(const Buffer& other);

    void                SetPreallocated(char* buffer, unsigned size);
    
    static bool         Cmp(Buffer& a, Buffer& b);
    bool                Cmp(const char* buffer, unsigned length_);
    bool                Cmp(const char* str);

    void                Lengthen(unsigned k);
    void                Shorten(unsigned k);
    
    void                Allocate(unsigned size, bool keepold = true);

    int                 Readf(const char* format, ...) const;
    unsigned            Writef(const char* fmt, ...);
    unsigned            Appendf(const char* fmt, ...);
    
    void                Write(const char* buffer, unsigned length);
    void                Write(const char* str);
    void                Write(Buffer& other);
    void                Write(ReadBuffer& other);

    void                Append(const char* buffer, unsigned length);
    void                Append(const char* str);
    void                Append(Buffer& other);
    void                Append(ReadBuffer& other);

    void                AppendLittle32(uint32_t x);
    void                AppendLittle64(uint64_t x);

    void                NullTerminate();
    void                Zero();

    void                SetLength(unsigned length);

    void                Init();
    unsigned            GetSize();
    char*               GetBuffer();
    unsigned            GetLength();
    unsigned            GetRemaining();
    char*               GetPosition();
    char                GetCharAt(unsigned i);
    uint32_t            GetChecksum();
    
    void                Clear();

    Buffer*             next;
    Buffer*             prev;
        
protected:
    char*               buffer;
    unsigned            size;
    unsigned            length;
    bool                preallocated;
    char                array[ARRAY_SIZE];
};

#endif
