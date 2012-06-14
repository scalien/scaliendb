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
 : listCacheNode(this), listPoolNode(this), listExpiryNode(this)
{
    Init();
}

void ShardLock::Init()
{
    locked = false;
    unlockTime = 0;
}

ShardLockManager::ShardLockManager()
{
    numLocked = 0;
    removeLocks.SetCallable(MFUNC(ShardLockManager, OnRemoveCachedLocksFromTree));
    removeLocks.SetDelay(LOCK_CHECK_FREQUENCY);
    expireLocks.SetCallable(MFUNC(ShardLockManager, OnExpireLocks));
    expireLocks.SetDelay(LOCK_CHECK_FREQUENCY);
}

void ShardLockManager::Init()
{
    EventLoop::Add(&removeLocks);
    EventLoop::Add(&expireLocks);
}

unsigned ShardLockManager::GetNumLocks()
{
    return numLocked;
}

unsigned ShardLockManager::GetLockTreeCount()
{
    return lockTree.GetCount();
}

unsigned ShardLockManager::GetLockCacheListLength()
{
    return lockCacheList.GetLength();
}

unsigned ShardLockManager::GetLockPoolListLength()
{
    return lockPoolList.GetLength();
}

unsigned ShardLockManager::GetLockExpiryListLength()
{
    return lockExpiryList.GetLength();
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
        newLock = NewLock();
        newLock->key.Write(key);
        lockTree.InsertAt(newLock, lock, cmpres);
        lock = newLock;
    }
    else if (lock->locked)
        return false;
    else
    {
        // in tree, not locked
        // in lock cache list
        ASSERT(lock->listCacheNode.next != lock);
        lockCacheList.Remove(lock);

        // not in lock expiry list
        ASSERT(lock->listExpiryNode.next == lock);
        // not in lock pool list
        ASSERT(lock->listPoolNode.next == lock);
    }

    // in tree, not locked
    lock->locked = true;
    numLocked++;
    lock->expireTime = EventLoop::Now() + LOCK_EXPIRE_TIME;
    // add to expiry list
    lockExpiryList.Append(lock);

    // in lock tree
    ASSERT(lock->treeNode.IsInTree());
    // not in lock cache list
    ASSERT(lock->listCacheNode.next == lock);
    // in lock expiry list
    ASSERT(lock->listExpiryNode.next != lock);
    // not in lock pool list
    ASSERT(lock->listPoolNode.next == lock);

    Log_Debug("Lock %R acquired.", &key);

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
    
    return lock->locked;
}

void ShardLockManager::Unlock(ReadBuffer key)
{
    Buffer      keyBuffer;
    ShardLock*  lock;
    
    keyBuffer.Write(key);
    lock = lockTree.Get(keyBuffer);
    
    if (!lock)
        return; // not in tree

    if (lock->locked)
        Unlock(lock);
}

void ShardLockManager::UnlockAll()
{
    lockCacheList.Clear();
    lockTree.DeleteTree();
    numLocked = 0;
}

void ShardLockManager::OnRemoveCachedLocksFromTree()
{
    uint64_t    now;
    ShardLock*  lock;
    
    now = EventLoop::Now();

    FOREACH_FIRST(lock, lockCacheList)
    {
        if (lock->unlockTime < now)
        {
            // should be unlocked
            ASSERT(!lock->locked);
            // not in lock expiry list
            ASSERT(lock->listExpiryNode.next == lock);
            // not in lock pool list
            ASSERT(lock->listPoolNode.next == lock);
            // in tree
            ASSERT(lock->treeNode.IsInTree());

            lockTree.Remove(lock);
            lockCacheList.Remove(lock);
            DeleteLock(lock);
        }
        else
            break;
    }

    EventLoop::Add(&removeLocks);
}

void ShardLockManager::OnExpireLocks()
{
    uint64_t    now;
    ShardLock*  lock;

    now = EventLoop::Now();

    FOREACH_FIRST(lock, lockExpiryList)
    {
        // should be locked
        ASSERT(lock->locked);
        // in tree
        ASSERT(lock->treeNode.IsInTree());
        // not in lock cache list
        ASSERT(lock->listCacheNode.next == lock);
        // not in lock pool list
        ASSERT(lock->listPoolNode.next == lock);

        if (lock->expireTime < now)
        {
            Log_Debug("Lock %B will be unlocked due to expiry", &lock->key);
            Unlock(lock); // modifies lockExpiryList
        }
        else
            break; // list is sorted because it is always appended
    }

    EventLoop::Add(&expireLocks);
}

 void ShardLockManager::Unlock(ShardLock* lock)
{
    // in tree, locked
    lock->locked = false;
    numLocked--;
    lock->unlockTime = EventLoop::Now() + LOCK_CACHE_TIME;

    // in lock expiry list
    ASSERT(lock->listExpiryNode.next != lock);
    lockExpiryList.Remove(lock);

    // not in the lock cache list
    ASSERT(lock->listCacheNode.next == lock);
    lockCacheList.Append(lock);

    // not in lock expiry list
    ASSERT(lock->listExpiryNode.next == lock);

    // in lock cache list
    ASSERT(lock->listCacheNode.next != lock);

    // in lock pool list
    ASSERT(lock->listPoolNode.next == lock);

    Log_Debug("Lock %B unlocked.", &lock->key);
}

ShardLock* ShardLockManager::NewLock()
{
    ShardLock*  lock;
    
    lock = lockPoolList.Pop();

    if (!lock)
        lock = new ShardLock;

    return lock;
}

void ShardLockManager::DeleteLock(ShardLock* lock)
{
    // not in lock tree
    ASSERT(!lock->treeNode.IsInTree());
    // not in lock cache list
    ASSERT(lock->listCacheNode.next == lock);
    // not in lock expiry list
    ASSERT(lock->listExpiryNode.next == lock);
    // not in lock pool list
    ASSERT(lock->listPoolNode.next == lock);

    if (lockPoolList.GetLength() < LOCK_POOL_SIZE)
    {
        // keep lock, put in object pool
        lockPoolList.Append(lock);
    }
    else
    {
        // don't keep lock, delete it for real
        delete lock;
    }
}
