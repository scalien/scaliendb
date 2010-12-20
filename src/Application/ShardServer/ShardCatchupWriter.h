#ifndef SHARDCATCHUPWRITER_H
#define SHARDCATCHUPWRITER_H

#include "Application/Common/CatchupMessage.h"

class ShardQuorumProcessor; // forward

/*
===============================================================================================

 ShardCatchupWriter

===============================================================================================
*/

class ShardCatchupWriter
{
public:
    void                    Init(ShardQuorumProcessor* quorumProcessor);
    void                    Reset();
    
    bool                    IsActive();

    void                    Begin(CatchupMessage& request);
    void                    Abort();

private:
    void                    SendFirst();
    void                    SendNext();
    void                    Commit();
    void                    OnWriteReadyness();
    uint64_t*               NextShard();

    bool                    isActive;
    uint64_t                nodeID;
    uint64_t                quorumID;
    uint64_t                shardID;
    uint64_t                paxosID;
//    StorageCursor*          cursor;
    ShardQuorumProcessor*   quorumProcessor;
};

#endif
