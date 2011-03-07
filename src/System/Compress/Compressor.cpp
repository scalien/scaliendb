#include "Compressor.h"

bool Compressor::Init()
{
    return (lzo_init() == LZO_E_OK);
}

bool Compressor::Compress(ReadBuffer uncompressed, Buffer& compressed)
{
    bool        ret;
    lzo_int     lzoRet;
    lzo_uint    len;

    compressed.Allocate(uncompressed.GetLength() + uncompressed.GetLength() / 16 + 64 + 3);

    lzoRet = lzo1x_1_compress(
     (const unsigned char*) uncompressed.GetBuffer(), (lzo_uint) uncompressed.GetLength(),
     (unsigned char*) compressed.GetBuffer(), &len, wrkmem);

    ret =  true;
    ret &= (lzoRet == LZO_E_OK);
    ret &= (len <= compressed.GetSize());

    if (ret)
        compressed.SetLength(len);
    else
        compressed.SetLength(0);

    return ret;
}

bool Compressor::Uncompress(ReadBuffer compressed, Buffer& uncompressed, uint32_t uncompressedLength)
{
    bool        ret;
    lzo_int     lzoRet;
    lzo_uint    len;

    uncompressed.Allocate(uncompressedLength);

    lzoRet = lzo1x_decompress(
     (const unsigned char*) compressed.GetBuffer(), (lzo_uint) compressed.GetLength(),
     (unsigned char*) uncompressed.GetBuffer(), &len, NULL);
    
    ret =  true;
    ret &= (lzoRet == LZO_E_OK);
    ret &= (len == uncompressedLength);
    
    if (ret)
        uncompressed.SetLength(uncompressedLength);
    else
        uncompressed.SetLength(0);

    return ret;
}
