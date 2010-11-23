#ifndef CONFIGSPLITSHARDMANAGER_H
#define CONFIGSPLITSHARDMANAGER_H

#include "Application/Common/ClusterMessage.h"

class ConfigServer;
class ConfigQuorum;
class ConfigShard;

#define SHARD_SPLIT_SIZE        1*GB

/*
===============================================================================================

 ConfigSplitShardManager

===============================================================================================
*/

class ConfigSplitShardManager
{
public:
    void            Init(ConfigServer* configServer);
    
    void            OnHeartbeatMessage(ClusterMessage& message);
    
private:
    bool            IsSplitCreating(ConfigQuorum* configQuorum);
    
    ConfigServer*   configServer;
};

#endif
