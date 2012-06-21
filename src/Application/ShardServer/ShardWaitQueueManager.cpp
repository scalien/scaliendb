#include "ShardWaitQueueManager.h"
#include "System/Events/EventLoop.h"
#include "Application/Common/ClientRequest.h"

static inline int KeyCmp(const Buffer& a, const Buffer& b)
{
    return Buffer::Cmp(a, b);
}

static inline const Buffer& Key(const ShardWaitQueue* waitQueue)
{
    return waitQueue->key;
}

ShardWaitQueueNode::ShardWaitQueueNode()
 : listWaitQueueNode(this), listPoolNode(this), listExpiryNode(this)
{
    Init();
}

void ShardWaitQueueNode::Init()
{
    expireTime = 0;
    request = NULL;
    waitQueue = NULL;
}

void ShardWaitQueueNode::AssertClear()
{
    // members cleared
    ASSERT(expireTime == 0);
    ASSERT(request == NULL);
    ASSERT(waitQueue == NULL);
    // not in a wait queue
    ASSERT(listWaitQueueNode.next == this);
    // not in expiry list
    ASSERT(listExpiryNode.next == this);
    // not in pool list
    ASSERT(listPoolNode.next == this);
}

ShardWaitQueue::ShardWaitQueue()
 : listCacheNode(this), listPoolNode(this)
{
    Init();
}

void ShardWaitQueue::Init()
{
    emptyTime = 0;
}

void ShardWaitQueue::AssertClear()
{
    // members cleared
    ASSERT(emptyTime == 0);
    ASSERT(key.GetLength() == 0);
    ASSERT(nodes.GetLength() == 0);
    // not in wait queue tree
    ASSERT(!treeNode.IsInTree());
    // not in wait queue cache list
    ASSERT(listCacheNode.next == this);
    // not in wait queue pool list
    ASSERT(listPoolNode.next == this);
}

ShardWaitQueueManager::ShardWaitQueueManager()
{
    removeCachedWaitQueues.SetCallable(MFUNC(ShardWaitQueueManager, OnRemoveCachedWaitQueues));
    removeCachedWaitQueues.SetDelay(WAITQUEUE_CHECK_FREQUENCY);
    expireRequests.SetCallable(MFUNC(ShardWaitQueueManager, OnExpireRequests));

    numWaiting = 0;
    waitExpireTime = WAITQUEUE_EXPIRE_TIME;
    maxCacheTime = WAITQUEUE_CACHE_TIME;
    maxCacheCount = WAITQUEUE_CACHE_COUNT;
    maxPoolCount = WAITQUEUE_POOL_COUNT;
}

void ShardWaitQueueManager::Init()
{
    EventLoop::Add(&removeCachedWaitQueues);
    EventLoop::Add(&expireRequests);
}

void ShardWaitQueueManager::Shutdown()
{
    FailAll();

    ASSERT(nodeExpiryList.GetLength() == 0);
    nodePoolList.DeleteList();
    queueCacheList.Clear();
    queueTree.DeleteTree();
    queuePoolList.DeleteList();
    UpdateExpireRequestsTimeout();
}

unsigned ShardWaitQueueManager::GetNumWaitQueues()
{
    ASSERT(queueTree.GetCount() - queueCacheList.GetLength() >= 0);
    return queueTree.GetCount() - queueCacheList.GetLength();
}

unsigned ShardWaitQueueManager::GetQueueCacheListLength()
{
    return queueCacheList.GetLength();
}

unsigned ShardWaitQueueManager::GetQueuePoolListLength()
{
    return queuePoolList.GetLength();
}

unsigned ShardWaitQueueManager::GetNodeExpiryListLength()
{
    return nodeExpiryList.GetLength();
}

unsigned ShardWaitQueueManager::GetNodePoolListLength()
{
    return nodePoolList.GetLength();
}

void ShardWaitQueueManager::SetWaitExpireTime(unsigned waitExpireTime_)
{
    waitExpireTime = waitExpireTime_;
}

void ShardWaitQueueManager::SetMaxCacheTime(unsigned maxCacheTime_)
{
    maxCacheTime = maxCacheTime_;
}

void ShardWaitQueueManager::SetMaxCacheCount(unsigned maxCacheCount_)
{
    maxCacheCount = maxCacheCount_;
}

void ShardWaitQueueManager::SetMaxPoolCount(unsigned maxPoolCount_)
{
    maxPoolCount = maxPoolCount_;
}

unsigned ShardWaitQueueManager::GetWaitExpireTime()
{
    return waitExpireTime;
}

unsigned ShardWaitQueueManager::GetMaxCacheTime()
{
    return maxCacheTime;
}

unsigned ShardWaitQueueManager::GetMaxCacheCount()
{
    return maxCacheCount;
}

unsigned ShardWaitQueueManager::GetMaxPoolCount()
{
    return maxPoolCount;
}

void ShardWaitQueueManager::Push(ClientRequest* request)
{
    int                 cmpres;
    ShardWaitQueue*     waitQueue;
    ShardWaitQueue*     newWaitQueue;
    ShardWaitQueueNode* waitQueueNode;

    waitQueue = queueTree.Locate(request->key, cmpres);
    
    if (!FOUND_IN_TREE(waitQueue, cmpres))
    {
        // not in tree
        newWaitQueue = NewWaitQueue();
        newWaitQueue->key.Write(request->key);
        queueTree.InsertAt(newWaitQueue, waitQueue, cmpres);
        waitQueue = newWaitQueue;
    }
    else if (waitQueue->nodes.GetLength() == 0)
    {
        // in tree, but wait queue is empty
        ASSERT(waitQueue->emptyTime > 0);
        // in wait queue cache list
        ASSERT(waitQueue->listCacheNode.next != waitQueue);
        
        // remove from cache list
        waitQueue->emptyTime = 0;
        queueCacheList.Remove(waitQueue);    }

    // in wait queue tree
    ASSERT(waitQueue->treeNode.IsInTree());
    // not in wait queue cache list
    ASSERT(waitQueue->listCacheNode.next == waitQueue);
    // not in wait queue pool list
    ASSERT(waitQueue->listPoolNode.next == waitQueue);

    waitQueueNode = NewWaitQueueNode();
    waitQueueNode->expireTime = EventLoop::Now() + waitExpireTime;
    // add to expiry list
    nodeExpiryList.Append(waitQueueNode);
    waitQueueNode->request = request;
    UpdateExpireRequestsTimeout();

    // add to actual wait queue
    waitQueue->nodes.Append(waitQueueNode);
    waitQueueNode->waitQueue = waitQueue;

    // in a wait queue
    ASSERT(waitQueueNode->listWaitQueueNode.next != waitQueueNode);
    // in expiry list
    ASSERT(waitQueueNode->listExpiryNode.next != waitQueueNode);
    // not in pool list
    ASSERT(waitQueueNode->listPoolNode.next == waitQueueNode);
}

ClientRequest* ShardWaitQueueManager::Pop(ReadBuffer key)
{
    int                 cmpres;
    Buffer              keyBuffer;
    ShardWaitQueue*     waitQueue;

    keyBuffer.Write(key);

    waitQueue = queueTree.Locate(keyBuffer, cmpres);
    
    if (!FOUND_IN_TREE(waitQueue, cmpres))
        return NULL;

    return Pop(waitQueue);
}

void ShardWaitQueueManager::FailAll()
{
    ShardWaitQueue*     waitQueue;
    ClientRequest*      request;

    FOREACH(waitQueue, queueTree)
    {
        while (request = Pop(waitQueue))
            Fail(request);
    }
}

void ShardWaitQueueManager::OnRemoveCachedWaitQueues()
{
    uint64_t        now;
    ShardWaitQueue* waitQueue;

    now = EventLoop::Now();
    
    FOREACH_FIRST(waitQueue, queueCacheList)
    {
        // emptyTime should be set
        ASSERT(waitQueue->emptyTime > 0);
        // wait queue should be empty
        ASSERT(waitQueue->nodes.GetLength() == 0);
        // in wait queue tree
        ASSERT(waitQueue->treeNode.IsInTree());
        // in wait queue cache list
        ASSERT(waitQueue->listCacheNode.next != waitQueue);
        // not in wait queue pool list
        ASSERT(waitQueue->listPoolNode.next == waitQueue);

        if (waitQueue->emptyTime < now || queueCacheList.GetLength() > maxCacheCount)
        {
            waitQueue->emptyTime = 0;
            waitQueue->key.Clear();
            queueTree.Remove(waitQueue);
            queueCacheList.Remove(waitQueue);
            DeleteWaitQueue(waitQueue);
        }
        else
            break; // list is sorted because it is always appended
    }

    EventLoop::Add(&removeCachedWaitQueues);
}

void ShardWaitQueueManager::OnExpireRequests()
{
    uint64_t            now;
    ShardWaitQueue*     waitQueue;
    ShardWaitQueueNode* waitQueueNode;
    ClientRequest*      request;

    now = EventLoop::Now();
    
    FOREACH_FIRST(waitQueueNode, nodeExpiryList)
    {
        // members cleared
        ASSERT(waitQueueNode->expireTime > 0);
        ASSERT(waitQueueNode->request != NULL);
        ASSERT(waitQueueNode->waitQueue != NULL);
        // in a wait queue
        ASSERT(waitQueueNode->listWaitQueueNode.next != waitQueueNode);
        // in expiry list
        ASSERT(waitQueueNode->listExpiryNode.next != waitQueueNode);
        // not in pool list
        ASSERT(waitQueueNode->listPoolNode.next == waitQueueNode);

        if (waitQueueNode->expireTime < now)
        {
            Log_Debug("Expiring start transaction for lock %B.", &waitQueueNode->waitQueue->key);
            waitQueue = waitQueueNode->waitQueue;
            // first in waitQueue
            ASSERT(waitQueue->nodes.GetLength() > 0);
            ASSERT(waitQueue->nodes.First() == waitQueueNode);
            
            request = Pop(waitQueue);
            Fail(request);
        }
        else
            break; // list is sorted because it is always appended
    }

    UpdateExpireRequestsTimeout();
}

ClientRequest* ShardWaitQueueManager::Pop(ShardWaitQueue* waitQueue)
{
    ShardWaitQueueNode* waitQueueNode;
    ClientRequest*      request;

    waitQueueNode = waitQueue->nodes.Pop();
    
    if (!waitQueueNode)
    {
        ASSERT(waitQueue->emptyTime > 0);
        // in wait queue cache list
        ASSERT(waitQueue->listCacheNode.next != waitQueue);
        return NULL;
    }
    else
    {
        ASSERT(waitQueue->emptyTime == 0);
        // not in wait queue cache list
        ASSERT(waitQueue->listCacheNode.next == waitQueue);
    }

    // members
    ASSERT(waitQueueNode->expireTime > 0);
    ASSERT(waitQueueNode->request);
    ASSERT(waitQueueNode->waitQueue == waitQueue);
    // not in a wait queue
    ASSERT(waitQueueNode->listWaitQueueNode.next == waitQueueNode);
    // in expiry list
    ASSERT(waitQueueNode->listExpiryNode.next != waitQueueNode);
    // not in pool list
    ASSERT(waitQueueNode->listPoolNode.next == waitQueueNode);

    request = waitQueueNode->request;
    nodeExpiryList.Remove(waitQueueNode);
    UpdateExpireRequestsTimeout();
    waitQueueNode->Init();
    DeleteWaitQueueNode(waitQueueNode);

    if (waitQueue->nodes.GetLength() == 0)
    {
        // wait queue is now empty, add to cache list
        waitQueue->emptyTime = EventLoop::Now() + maxCacheTime;
        queueCacheList.Append(waitQueue);
    }

    return request;
}

void ShardWaitQueueManager::Fail(ClientRequest* request)
{
    request->response.Failed();
    request->OnComplete();
}

ShardWaitQueue* ShardWaitQueueManager::NewWaitQueue()
{
    ShardWaitQueue* waitQueue;
    
    waitQueue = queuePoolList.Pop();

    if (!waitQueue)
        waitQueue = new ShardWaitQueue;

    waitQueue->Init();
    waitQueue->AssertClear();

    return waitQueue;
}

void ShardWaitQueueManager::DeleteWaitQueue(ShardWaitQueue* waitQueue)
{
    waitQueue->AssertClear();

    if (queuePoolList.GetLength() < maxPoolCount)
    {
        // keep, put in object pool
        queuePoolList.Append(waitQueue);
    }
    else
    {
        // don't keep, delete it for real
        delete waitQueue;
    }
}

ShardWaitQueueNode* ShardWaitQueueManager::NewWaitQueueNode()
{
    ShardWaitQueueNode* waitQueueNode;
    
    waitQueueNode = nodePoolList.Pop();

    if (!waitQueueNode)
        waitQueueNode = new ShardWaitQueueNode;

    waitQueueNode->Init();
    waitQueueNode->AssertClear();

    return waitQueueNode;
}

void ShardWaitQueueManager::DeleteWaitQueueNode(ShardWaitQueueNode* waitQueueNode)
{
    waitQueueNode->AssertClear();

    if (nodePoolList.GetLength() < maxPoolCount)
    {
        // keep, put in object pool
        nodePoolList.Append(waitQueueNode);
    }
    else
    {
        // don't keep, delete it for real
        delete waitQueueNode;
    }
}

void ShardWaitQueueManager::UpdateExpireRequestsTimeout()
{
    ShardWaitQueueNode* waitQueueNode;

    EventLoop::Remove(&expireRequests);

    waitQueueNode = nodeExpiryList.First();
    if (waitQueueNode)
    {
        ASSERT(waitQueueNode->expireTime > 0);
        expireRequests.SetExpireTime(waitQueueNode->expireTime);
        EventLoop::Add(&expireRequests);
    }
}
