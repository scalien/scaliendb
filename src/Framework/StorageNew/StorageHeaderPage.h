#define STORAGE_HEADER_PAGE_SIZE        4096

class StorageHeaderPage
{
public:
    uint32_t    GetPageSize();

    void        SetShardID(uint64_t shardID);
    void        SetLogSegmentID(uint64_t segmentID);
    void        SetChunkID(uint64_t chunkID);
    void        SetNumKeys(uint64_t numKeys);

    void        SetVersion(uint32_t version);
    void        SetUseBloomFilter(bool useBloomFilter);
    void        SetIndexPageOffset(uint64_t indexOffset);
    void        SetIndexPageSize(uint32_t indexSize);
    void        SetBloomPageOffset(uint64_t bloomOffset);
    void        SetBloomPageSize(uint32_t bloomSize);

    void        Write(Buffer& buffer);
    bool        Read(FD fd, Buffer& buffer);
};
