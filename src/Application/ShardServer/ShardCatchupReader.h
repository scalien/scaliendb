#ifndef SHARDCATCHUPREADER_H
#define SHARDCATCHUPREADER_H

#include "Application/Common/CatchupMessage.h"
#include "Framework/Storage/StorageEnvironment.h"

class ShardQuorumProcessor;

/*
===============================================================================================

 ShardCatchupReader

===============================================================================================
*/

class ShardCatchupReader
{
public:
    void                    Init(ShardQuorumProcessor* quorumProcessor);
    
    bool                    IsActive();

    void                    Begin();
    void                    Abort();    
    
    void                    OnBeginShard(CatchupMessage& msg);
    void                    OnSet(CatchupMessage& msg);
    void                    OnDelete(CatchupMessage& msg);
    void                    OnCommit(CatchupMessage& msg);
    void                    OnAbort(CatchupMessage& msg);

private:
    bool                    isActive;
    uint64_t                shardID;
    ShardQuorumProcessor*   quorumProcessor;
    StorageEnvironment*     environment;
};

#endif
