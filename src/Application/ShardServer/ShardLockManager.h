#ifndef SHARDLOCKMANAGER_H
#define SHARDLOCKMANAGER_H

#include "System/Buffers/Buffer.h"
#include "System/Events/Countdown.h"
#include "System/Containers/InTreeMap.h"
#include "System/Containers/InList.h"

#define LOCK_CACHE_TIME             (10*1000) // 10sec
#define LOCK_CACHE_CHECK_FREQUENCY  (1000)  // 1sec

/*
===============================================================================================

 ShardLock
 
===============================================================================================
*/

class ShardLock
{
    typedef InTreeNode<ShardLock> TreeNode;

public:
    ShardLock();
    
    TreeNode    treeNode;    
    Buffer      key;
    bool        locked;
    uint64_t    unlockTime;
    
    ShardLock*  prev;
    ShardLock*  next;
};

/*
===============================================================================================

 ShardLockManager
 
===============================================================================================
*/

class ShardLockManager
{
    typedef InTreeMap<ShardLock> LockTree;
    typedef InList<ShardLock> LockList;

    // create ShardLock cache

public:
    ShardLockManager();
    
    unsigned    GetNumLocks();
    bool        TryLock(ReadBuffer key);
    bool        IsLocked(ReadBuffer key);
    void        Unlock(ReadBuffer key);
    void        UnlockAll();
    void        OnRemoveLocks();
    
private:
    unsigned    numLocked;
    LockTree    lockTree;
    LockList    lockList;
    Countdown   removeLocks;
};

#endif
