#include "BloomFilter.h"
#include "System/Log.h"

static int32_t table32[256];
static int32_t table40[256];
static int32_t table48[256];
static int32_t table54[256];
static int32_t mods[BLOOMFILTER_P_DEGREE];

static int32_t HashRabin(ReadBuffer& r)
{
    int32_t         w;
    unsigned        s, start;
    
    w = 0;
    start = 0;

    if (r.GetLength() == 0)
        return 0;

    if (r.GetLength() % 2 == 1)
    {
        w = r.GetCharAt(0) & 0xFFFF;
        start = 1;
    }

    for (s = start; s < (r.GetLength() - 1); s += 2)
    {
        w = table32[w & 0xFF] ^
            table40[(((uint32_t)w) >> 8)  & 0xFF] ^
            table48[(((uint32_t)w) >> 16) & 0xFF] ^
            table54[(((uint32_t)w) >> 24) & 0xFF] ^
            ((r.GetCharAt(s) & 0xFFFF) << 16) ^ (r.GetCharAt(s + 1) & 0xFFFF);
    }

    return w;
}

static int32_t HashRabin(int32_t r)
{
    int32_t w;

    w = r;

    w =
     table32[w & 0xFF] ^
     table40[(((uint32_t)w) >> 8)  & 0xFF] ^
     table48[(((uint32_t)w) >> 16) & 0xFF] ^
     table54[(((uint32_t)w) >> 24) & 0xFF] ^ r;

    return w;
}

void BloomFilter::StaticInit()
{
    unsigned i, j, c;

    mods[0] = BLOOMFILTER_POLYNOMIAL;
    for (i = 1; i < BLOOMFILTER_P_DEGREE; i++)
    {
        mods[i] = mods[i - 1] << 1;
        if ((mods[i - 1] & BLOOMFILTER_X_P_DEGREE) != 0)
            mods[i] ^= BLOOMFILTER_POLYNOMIAL;
    }

    for (i = 0; i < 256; i++)
    {
        c = i;
        for (j = 0; j < 8 && c != 0; j++)
        {
            if ((c & 1) != 0)
            {
                table32[i] ^= mods[j];
                table40[i] ^= mods[j + 8];
                table48[i] ^= mods[j + 16];
                table54[i] ^= mods[j + 24];
            }
            c = ((unsigned)(c) >> 1);
        }
    }
}

void BloomFilter::SetSize(uint32_t size)
{
    buffer.Allocate(size);
    buffer.SetLength(size);
    buffer.Zero();
}

void BloomFilter::Add(ReadBuffer& key)
{
    int32_t     hash;
    unsigned    i, k, j, bitindex;
    char*       p;

    hash = HashRabin(key);
    for (i = 0; i < BLOOMFILTER_NUM_FUNCTIONS; i++)
    {
        bitindex = GetHash(i, hash);
        k = bitindex / 8;
        j = bitindex % 8;
        p = buffer.GetBuffer() + k;
        *p |= (1 << j);
    }
}

void BloomFilter::SetBuffer(ReadBuffer& buffer_)
{
    buffer.Write(buffer_);
}

bool BloomFilter::Check(ReadBuffer& key)
{
    bool        res;
    int32_t     hash;
    unsigned    i, k, j, bitindex;
    char        c;

    res = true;
    hash = HashRabin(key);
    for (i = 0; i < BLOOMFILTER_NUM_FUNCTIONS && res; i++)
    {
        bitindex = GetHash(i, hash);
        k = bitindex / 8;
        j = bitindex % 8;
        c = *(buffer.GetBuffer() + k);
        res &= (((c >> j) & 1));
    }
    
    return res;
}

Buffer& BloomFilter::GetBuffer()
{
    return buffer;
}

void BloomFilter::Reset()
{
    buffer.Reset();
}

int32_t BloomFilter::GetHash(unsigned fnum, int32_t original)
{
    unsigned    i;
    int32_t     hash;

    hash = original;
    for (i = 0; i < fnum; i++)
        hash = HashRabin(hash);

    hash = hash % (buffer.GetLength() * 8);

    if (hash < 0)
        hash = -hash;

    return hash;
}

unsigned BloomFilter::BitCount(uint32_t u)
{
    uint32_t    count;

    count = u 
          - ((u >> 1) & 033333333333)
          - ((u >> 2) & 011111111111);
    return ((count + (count >> 3)) & 030707070707) % 63;
}