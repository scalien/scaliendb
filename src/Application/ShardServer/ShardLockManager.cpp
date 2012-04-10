#include "ShardLockManager.h"
#include "System/Events/EventLoop.h"

static inline int KeyCmp(const Buffer& a, const Buffer& b)
{
    return Buffer::Cmp(a, b);
}

static inline const Buffer& Key(const ShardLock* lock)
{
    return lock->key;
}

ShardLock::ShardLock()
{
    locked = false;
    unlockTime = 0;
    next = prev = this;
}

ShardLockManager::ShardLockManager()
{
    numLocked = 0;
    removeLocks.SetCallable(MFUNC(ShardLockManager, OnRemoveLocks));
    removeLocks.SetDelay(LOCK_CACHE_CHECK_FREQUENCY);
}

unsigned ShardLockManager::GetNumLocks()
{
    return numLocked;
}

bool ShardLockManager::TryLock(ReadBuffer key)
{
    int         cmpres;
    Buffer      keyBuffer;
    ShardLock*  lock;
    ShardLock*  newLock;
    
    keyBuffer.Write(key);
    
    lock = lockTree.Locate(keyBuffer, cmpres);
    
    if (!FOUND_IN_TREE(lock, cmpres))
    {
        // not in tree
        newLock = new ShardLock;
        newLock->key.Write(key);
        newLock->locked = true;
        numLocked++;
        lockTree.InsertAt(newLock, lock, cmpres);
        return true;
    }
    
    // already in tree
    if (lock->locked)
        return false;
    lock->locked = true;
    numLocked++;
    return true;
}

bool ShardLockManager::IsLocked(ReadBuffer key)
{
    Buffer      keyBuffer;
    ShardLock*  lock;
    
    keyBuffer.Write(key);
    
    lock = lockTree.Get(keyBuffer);
    if (!lock)
        return false;
        
    if (lock->locked)
        return true;
    
    return false;
}

void ShardLockManager::Unlock(ReadBuffer key)
{
    Buffer      keyBuffer;
    ShardLock*  lock;
    
    keyBuffer.Write(key);
    lock = lockTree.Get(keyBuffer);
    
    if (!lock)
        return; // not in tree
    // already in tree
    
    ASSERT(numLocked > 0);

    lock->locked = false;
    numLocked--;
    lock->unlockTime = EventLoop::Now();

    // maintain in lock list
    if (lock->prev != lock)
        lockList.Remove(lock);
    lockList.Append(lock);
}

void ShardLockManager::UnlockAll()
{
    lockList.Clear();
    lockTree.DeleteTree();
    numLocked = 0;
}

void ShardLockManager::OnRemoveLocks()
{
    uint64_t    t;
    ShardLock*  lock;
    
    t = EventLoop::Now() + LOCK_CACHE_TIME;

    FOREACH_FIRST(lock, lockList)
    {
        if (lock->unlockTime < t)
        {
            lockTree.Remove(lock);
            lockList.Remove(lock);
            delete lock;
        }
        else
            return;
    }
}
