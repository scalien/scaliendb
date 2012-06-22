#include "ShardLockManager.h"
#include "ShardTransactionManager.h"
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
    expireTime = 0;
    session = NULL;
}

ShardLockManager::ShardLockManager()
{
    removeCachedLocks.SetCallable(MFUNC(ShardLockManager, OnRemoveCachedLocks));
    removeCachedLocks.SetDelay(LOCK_CHECK_FREQUENCY);
    
    numLocked = 0;
    lockExpireTime = LOCK_EXPIRE_TIME;
    maxCacheTime = LOCK_CACHE_TIME;
    maxCacheCount = LOCK_CACHE_COUNT;
    maxPoolCount = LOCK_POOL_COUNT;
}

void ShardLockManager::Init(const Callable& onExpireLocks)
{
    expireLocks.SetCallable(onExpireLocks);
    EventLoop::Add(&removeCachedLocks);
}

void ShardLockManager::Shutdown()
{
    UnlockAll();
    numLocked = 0;
}

unsigned ShardLockManager::GetNumLocks()
{
    return numLocked;
}

unsigned ShardLockManager::GetTreeCount()
{
    return lockTree.GetCount();
}

unsigned ShardLockManager::GetCacheListLength()
{
    return lockCacheList.GetLength();
}

unsigned ShardLockManager::GetPoolListLength()
{
    return lockPoolList.GetLength();
}

unsigned ShardLockManager::GetExpiryListLength()
{
    return lockExpiryList.GetLength();
}

void ShardLockManager::SetLockExpireTime(unsigned lockExpireTime_)
{
    lockExpireTime = lockExpireTime_;
}

void ShardLockManager::SetMaxCacheTime(unsigned maxCacheTime_)
{
    maxCacheTime = maxCacheTime_;
}

void ShardLockManager::SetMaxCacheCount(unsigned maxCacheCount_)
{
    maxCacheCount = maxCacheCount_;
}

void ShardLockManager::SetMaxPoolCount(unsigned maxPoolCount_)
{
    maxPoolCount = maxPoolCount_;
}

unsigned ShardLockManager::GetLockExpireTime()
{
    return lockExpireTime;
}

unsigned ShardLockManager::GetMaxCacheTime()
{
    return maxCacheTime;
}

unsigned ShardLockManager::GetMaxCacheCount()
{
    return maxCacheCount;
}

unsigned ShardLockManager::GetMaxPoolCount()
{
    return maxPoolCount;
}

bool ShardLockManager::TryLock(ReadBuffer key, ClientSession* session)
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
    lock->session = session;
    numLocked++;
    lock->expireTime = EventLoop::Now() + lockExpireTime;
    // add to expiry list
    lockExpiryList.Append(lock);
    UpdateExpireLockTimeout();

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
    lockExpiryList.Clear();
    lockPoolList.Clear();
    lockTree.DeleteTree();
    numLocked = 0;
    UpdateExpireLockTimeout();
}

void ShardLockManager::OnRemoveCachedLocks()
{
    uint64_t    now;
    ShardLock*  lock;
    
    now = EventLoop::Now();

    FOREACH_FIRST(lock, lockCacheList)
    {
        // should be unlocked
        ASSERT(!lock->locked);
        // session should be NULL
        ASSERT(lock->session == NULL);
        // not in lock expiry list
        ASSERT(lock->listExpiryNode.next == lock);
        // not in lock pool list
        ASSERT(lock->listPoolNode.next == lock);
        // in tree
        ASSERT(lock->treeNode.IsInTree());

        if (lock->unlockTime < now || lockCacheList.GetLength() > maxCacheCount)
        {
            lockTree.Remove(lock);
            lockCacheList.Remove(lock);
            DeleteLock(lock);
        }
        else
            break;
    }

    EventLoop::Add(&removeCachedLocks);
}

void ShardLockManager::OnExpireLocks(ShardTransactionManager* transactionManager)
{
    uint64_t        now;
    ShardLock*      lock;
    ClientSession*  session;

    now = EventLoop::Now();

    FOREACH_FIRST(lock, lockExpiryList)
    {
        // should be locked
        ASSERT(lock->locked);
        // session is set
        ASSERT(lock->session);
        // in tree
        ASSERT(lock->treeNode.IsInTree());
        // not in lock cache list
        ASSERT(lock->listCacheNode.next == lock);
        // not in lock pool list
        ASSERT(lock->listPoolNode.next == lock);

        if (lock->expireTime < now)
        {
            Log_Debug("Lock %B will be unlocked due to expiry", &lock->key);
            session = lock->session;
            Unlock(lock); // modifies lockExpiryList
            transactionManager->ClearSessionTransaction(session);
        }
        else
            break; // list is sorted because it is always appended
    }

    UpdateExpireLockTimeout();
}

 void ShardLockManager::Unlock(ShardLock* lock)
{
    // in tree, locked
    lock->locked = false;
    lock->session = NULL;
    numLocked--;
    lock->unlockTime = EventLoop::Now() + maxCacheTime;

    // in lock expiry list
    ASSERT(lock->listExpiryNode.next != lock);
    lockExpiryList.Remove(lock);
    UpdateExpireLockTimeout();

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

    lock->Init();

    // members cleared
    ASSERT(lock->locked == false);
    ASSERT(lock->session == NULL);
    ASSERT(lock->expireTime == 0);
    // not in lock tree
    ASSERT(!lock->treeNode.IsInTree());
    // not in lock cache list
    ASSERT(lock->listCacheNode.next == lock);
    // not in lock expiry list
    ASSERT(lock->listExpiryNode.next == lock);
    // not in lock pool list
    ASSERT(lock->listPoolNode.next == lock);

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

    if (lockPoolList.GetLength() < maxPoolCount)
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

void ShardLockManager::UpdateExpireLockTimeout()
{
    ShardLock* lock;

    EventLoop::Remove(&expireLocks);

    lock = lockExpiryList.First();
    if (lock)
    {
        ASSERT(lock->expireTime > 0);
        expireLocks.SetExpireTime(lock->expireTime);
        EventLoop::Add(&expireLocks);
    }
}
