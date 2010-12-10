#ifndef BLOOMFILTER_H
#define BLOOMFILTER_H

#include "System/Buffers/Buffer.h"

#define BLOOMFILTER_NUM_FUNCTIONS   16
#define BLOOMFILTER_POLYNOMIAL      0x000001C7
#define BLOOMFILTER_P_DEGREE        32
#define BLOOMFILTER_X_P_DEGREE      (1 << 31)

/*
===============================================================================================

 BloomFilter

===============================================================================================
*/

class BloomFilter
{
public:
    static void     StaticInit();
    
    static uint32_t RecommendNumBytes(uint32_t numKeys);

    void            SetSize(uint32_t size);
    void            Add(ReadBuffer& key);

    void            StartReading(Buffer& buffer);
    bool            Check(ReadBuffer& key);

    Buffer&         GetBuffer();

private:
    int32_t         GetHash(unsigned fnum, int32_t original);

    Buffer          buffer;
};

#endif
