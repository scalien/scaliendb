#ifndef SHARDCATCHUPREADER_H
#define SHARDCATCHUPREADER_H

#include "Framework/Storage/StorageShard.h"
#include "Application/Common/CatchupMessage.h"

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
    void                    OnKeyValue(CatchupMessage& msg);
    void                    OnCommit(CatchupMessage& msg);
    void                    OnAbort(CatchupMessage& msg);

private:
    bool                    isActive;
    StorageShard*           shard;
    ShardQuorumProcessor*   quorumProcessor;
};

#endif
