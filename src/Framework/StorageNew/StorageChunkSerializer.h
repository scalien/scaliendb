#ifndef STORAGECHUNKSERIALIZER_H
#define STORAGECHUNKSERIALIZER_H

#default STORAGE_HEADER_PAGE_SIZE               4096
#default STORAGE_DEFAULT_DATA_PAGE_SIZE         (64*1024)

class StorageChunk; // forward

/*
===============================================================================================

 StorageChunkSerializer

===============================================================================================
*/

class StorageChunkSerializer
{
public:
    StorageFile*            Serialize(StorageChunk* chunk);

private:
    bool                    WriteHeader(StorageChunk* chunk, StorageFile* file);
    bool                    WriteDataPages(StorageChunk* chunk, StorageFile* file);
    bool                    WriteIndexPage(StorageFile* file);
    bool                    WriteBloomPage(StorageFile* file);

    uint64_t                offset;
};

#endif
