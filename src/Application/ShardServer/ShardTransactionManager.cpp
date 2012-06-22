#include "ShardTransactionManager.h"
#include "System/Config.h"
#include "ShardServer.h"

void ShardTransactionManager::Init(ShardServer* shardServer_)
{
    lockManager.Init(MFUNC(ShardTransactionManager, OnTimeout));
    waitQueueManager.Init(MFUNC(ShardTransactionManager, OnTimeout));
    shardServer = shardServer_;

    lockManager.SetLockExpireTime(configFile.GetIntValue("      lock.expireTime", LOCK_EXPIRE_TIME));
    lockManager.SetMaxCacheTime(configFile.GetIntValue("        lock.maxCacheTime", LOCK_CACHE_TIME));
    lockManager.SetMaxCacheCount(configFile.GetIntValue("       lock.maxCacheCount", LOCK_CACHE_COUNT));
    lockManager.SetMaxPoolCount(configFile.GetIntValue("        lock.maxPoolCount", LOCK_POOL_COUNT));

    waitQueueManager.SetWaitExpireTime(configFile.GetIntValue(" waitQueue.expireTime", WAITQUEUE_EXPIRE_TIME));
    waitQueueManager.SetMaxCacheTime(configFile.GetIntValue("   waitQueue.maxCacheTime", WAITQUEUE_CACHE_TIME));
    waitQueueManager.SetMaxCacheCount(configFile.GetIntValue("  waitQueue.maxCacheCount", WAITQUEUE_CACHE_COUNT));
    waitQueueManager.SetMaxPoolCount(configFile.GetIntValue("   waitQueue.maxPoolCount", WAITQUEUE_POOL_COUNT));
}

void ShardTransactionManager::Shutdown()
{
    waitQueueManager.Shutdown();
    lockManager.Shutdown();
}

ShardLockManager* ShardTransactionManager::GetLockManager()
{
    return &lockManager;
}

ShardWaitQueueManager* ShardTransactionManager::GetWaitQueueManager()
{
    return &waitQueueManager;
}

bool ShardTransactionManager::ClearSessionTransaction(ClientSession* session)
{
    Buffer          lockKey;
    ClientRequest*  request;

    if (session->transaction.GetLength() > 0)
    {
        if (session->transaction.Last()->type == CLIENTREQUEST_COMMIT_TRANSACTION)
        {
            // client already sent a commit, and shard messages are queued for replication
            // replication has possibly began, cannot clear!
            Log_Debug("Cannot clear session transaction due to possibly replicating commit...");
            return false;
        }
    }

    FOREACH_POP(request, session->transaction)
    {
        ASSERT(request->type != CLIENTREQUEST_UNDEFINED);
        request->response.NoResponse();
        request->OnComplete(); // request deletes itself
    }

    lockManager.Unlock(session->lockKey);
    lockKey.Write(session->lockKey);
    session->Init();

    request = waitQueueManager.Pop(lockKey);
    if (request)
        shardServer->OnClientRequest(request);

    return true;
}

void ShardTransactionManager::OnTimeout()
{
    lockManager.OnExpireLocks(this);
    waitQueueManager.OnExpireRequests();
}
