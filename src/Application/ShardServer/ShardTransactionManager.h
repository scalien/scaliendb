#ifndef SHARDTRANSACTIONMANAGER_H
#define SHARDTRANSACTIONMANAGER_H

#include "ShardLockManager.h"
#include "ShardWaitQueueManager.h"
#include "Application/Common/ClientSession.h"

class ShardServer;

/*
===============================================================================================

 ShardTransactionManager
 
===============================================================================================
*/

class ShardTransactionManager
{
public:
    void                    Init(ShardServer* shardServer);
    void                    Shutdown();

    ShardLockManager*       GetLockManager();
    ShardWaitQueueManager*  GetWaitQueueManager();

    bool                    ClearSessionTransaction(ClientSession* session);

    void                    OnTimeout();

private:
    ShardLockManager        lockManager;
    ShardWaitQueueManager   waitQueueManager;
    ShardServer*            shardServer;
};

#endif
