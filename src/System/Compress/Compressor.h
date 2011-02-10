#ifndef COMPRESSOR_H
#define COMPRESSOR_H

#include "System/Buffers/Buffer.h"
#include "minilzo.h"

#define HEAP_ALLOC(var,size) \
 lzo_align_t __LZO_MMODEL var [ ((size) + (sizeof(lzo_align_t) - 1)) / sizeof(lzo_align_t) ]

class Compressor
{
public:
    static bool     Init();
    
    bool            Compress(ReadBuffer uncompressed, Buffer& compressed);
    bool            Uncompress(ReadBuffer compressed, Buffer& uncompressed, uint32_t uncompressedLength);

private:
    HEAP_ALLOC(wrkmem, LZO1X_1_MEM_COMPRESS);
};

#endif
