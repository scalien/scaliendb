#ifndef SHARDLOCKMANAGER_H
#define SHARDLOCKMANAGER_H

#include "System/Buffers/Buffer.h"
#include "System/Events/Countdown.h"
#include "System/Containers/InTreeMap.h"
#include "System/Containers/InNodeList.h"

#define LOCK_CACHE_TIME             (60*1000)   // msec
#define LOCK_CHECK_FREQUENCY        (1000)      // msec
#define LOCK_EXPIRE_TIME            (3000)      // msec
#define LOCK_POOL_SIZE              (10*1000)

/*
===============================================================================================

 ShardLock

- the C# client library should try again a couple of times if it fails to acquire a lock
 
===============================================================================================
*/

class ShardLock
{
    typedef InTreeNode<ShardLock> TreeNode;
    typedef InListNode<ShardLock> ListNode;

public:
    ShardLock();

    void            Init();
    
    TreeNode        treeNode;
    ListNode        listCacheNode;
    ListNode        listPoolNode;
    ListNode        listExpiryNode;

    bool            locked;
    Buffer          key;
    uint64_t        expireTime;
    uint64_t        unlockTime;
};

/*
===============================================================================================

 ShardLockManager
 
===============================================================================================
*/

class ShardLockManager
{
    typedef InTreeMap<ShardLock>                                LockTree;
    typedef InNodeList<ShardLock, &ShardLock::listCacheNode>    LockCacheList;
    typedef InNodeList<ShardLock, &ShardLock::listExpiryNode>   LockExpiryList;
    typedef InNodeList<ShardLock, &ShardLock::listPoolNode>     LockPoolList;

    // create ShardLock cache

public:
    ShardLockManager();

    void            Init();
    
    unsigned        GetNumLocks();
    unsigned        GetLockTreeCount();
    unsigned        GetLockCacheListLength();
    unsigned        GetLockPoolListLength();
    unsigned        GetLockExpiryListLength();

    bool            TryLock(ReadBuffer key);
    bool            IsLocked(ReadBuffer key);
    void            Unlock(ReadBuffer key);
    void            UnlockAll();
    void            OnRemoveCachedLocksFromTree();
    void            OnExpireLocks();
    
private:
    void            Unlock(ShardLock* lock);

    ShardLock*      NewLock();
    void            DeleteLock(ShardLock* lock);

    unsigned        numLocked;
    LockTree        lockTree;
    LockCacheList   lockCacheList;
    LockPoolList    lockPoolList;
    LockExpiryList  lockExpiryList;
    Countdown       removeLocks;
    Countdown       expireLocks;
};

#endif
