#ifndef STORAGESHARD_H
#define STORAGESHARD_H

#include "System/IO/FD.h"
#include "System/Containers/InTreeMap.h"
#include "StorageFile.h"
#include "StorageCursor.h"
#include "StorageFileIndex.h"

class StorageDatabase; // forward

/*
===============================================================================

 StorageShard

===============================================================================
*/

class StorageShard
{
    typedef InTreeMap<StorageFileIndex> FileIndexMap;

public:
    ~StorageShard();
    
    const char*             GetName();
    uint64_t                GetSize();
    bool                    GetMidpoint(ReadBuffer& key);
    ReadBuffer              FirstKey();

    void                    Open(const char* dir, const char* name);
    void                    Commit(bool recovery = true, bool flush = true);
    void                    Close();
        
    bool                    Get(ReadBuffer& key, ReadBuffer& value);
    bool                    Set(ReadBuffer& key, ReadBuffer& value, bool copy = true);
    bool                    Delete(ReadBuffer& key);

    StorageShard*           SplitShard(uint64_t newShardID, ReadBuffer& startKey);

private:
    bool                    CreateDir(const char* dir, const char* name);
    void                    WritePath(Buffer& buffer, uint32_t index);
    static void             WritePath(Buffer& buffer, Buffer& path, uint32_t index);
    uint64_t                ReadTOC(uint32_t length);
    void                    PerformRecovery(uint32_t length);
    void                    WriteBackPages(InList<Buffer>& pages);
    void                    DeleteGarbageFiles();
    uint64_t                RebuildTOC();
    void                    WriteRecoveryPrefix();
    void                    WriteRecoveryPostfix();
    void                    WriteTOC();
    void                    WriteData();    
    StorageFileIndex*       Locate(ReadBuffer& key);
    void                    SplitFile(StorageFile* file);

    StorageDataPage*        CursorBegin(ReadBuffer& key, Buffer& nextKey);

    void                    CommitPhase1();
    void                    CommitPhase2();
    void                    CommitPhase3();
    void                    CommitPhase4();
    
    FD                      tocFD;
    FD                      recoveryFD;
    uint64_t                shardID;
    uint64_t                shardSize;
    uint32_t                prevCommitStorageFileIndex;
    uint32_t                nextStorageFileIndex;
    Buffer                  name;
    Buffer                  path;
    Buffer                  tocFilepath;
    Buffer                  recoveryFilepath;
    Buffer                  buffer;
    FileIndexMap            files;
    
    friend class StorageCursor;
    friend class StorageTable;
};

#endif
