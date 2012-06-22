#ifndef SHARDWAITQUEUEMANAGER_H
#define SHARDWAITQUEUEMANAGER_H

#include "System/Buffers/Buffer.h"
#include "System/Events/Countdown.h"
#include "System/Containers/InTreeMap.h"
#include "System/Containers/InNodeList.h"

#define WAITQUEUE_CHECK_FREQUENCY        (1000)      // msec
#define WAITQUEUE_EXPIRE_TIME            (3000)      // msec
#define WAITQUEUE_CACHE_TIME             (60*1000)   // msec
#define WAITQUEUE_CACHE_COUNT            (10*1000)
#define WAITQUEUE_POOL_COUNT             (10*1000)

class ClientRequest;
class ShardWaitQueue;
class ShardServer;

/*
===============================================================================================

 ShardWaitQueueListNode
 
===============================================================================================
*/

class ShardWaitQueueNode
{
    typedef InListNode<ShardWaitQueueNode>  ListNode;
    typedef ShardWaitQueue                  WaitQueue;

public:
    ShardWaitQueueNode();

    void            Init();
    void            AssertClear();

    uint64_t        expireTime;
    ClientRequest*  request;
    WaitQueue*      waitQueue;

    ListNode        listWaitQueueNode;
    ListNode        listExpiryNode;
    ListNode        listPoolNode;
};

/*
===============================================================================================

 ShardWaitQueueTreeNode
 
===============================================================================================
*/

class ShardWaitQueue
{
    typedef InTreeNode<ShardWaitQueue> TreeNode;
    typedef InListNode<ShardWaitQueue> ListNode;
    typedef InNodeList<ShardWaitQueueNode, &ShardWaitQueueNode::listWaitQueueNode> Nodes;

public:
    ShardWaitQueue();

    void            Init();
    void            AssertClear();

    Buffer          key;
    uint64_t        emptyTime; // when this waitQueue was emptied
    Nodes           nodes;

    TreeNode        treeNode;
    ListNode        listCacheNode;
    ListNode        listPoolNode;
};

/*
===============================================================================================

 ShardWaitQueueManager
 
===============================================================================================
*/

class ShardWaitQueueManager
{
    typedef InTreeMap<ShardWaitQueue>                                           QueueTree;
    typedef InNodeList<ShardWaitQueueNode, &ShardWaitQueueNode::listExpiryNode> NodeExpiryList;
    typedef InNodeList<ShardWaitQueueNode, &ShardWaitQueueNode::listPoolNode>   NodePoolList;
    typedef InNodeList<ShardWaitQueue, &ShardWaitQueue::listCacheNode>          QueueCacheList;
    typedef InNodeList<ShardWaitQueue, &ShardWaitQueue::listPoolNode>           QueuePoolList;

public:
    ShardWaitQueueManager();

    void                Init(ShardServer* shardServer);
    void                Shutdown();

    // internal data structures stats
    unsigned            GetNumWaitQueues();
    unsigned            GetQueueCacheListLength();
    unsigned            GetQueuePoolListLength();
    unsigned            GetNodeExpiryListLength();
    unsigned            GetNodePoolListLength();

    // set parameters
    void                SetWaitExpireTime(unsigned lockExpireTime);
    void                SetMaxCacheTime(unsigned maxCacheTime);
    void                SetMaxCacheCount(unsigned maxCacheCount);
    void                SetMaxPoolCount(unsigned maxPoolCount);

    // get parameters
    unsigned            GetWaitExpireTime();
    unsigned            GetMaxCacheTime();
    unsigned            GetMaxCacheCount();
    unsigned            GetMaxPoolCount();

    // waitqueue interface
    void                Push(ClientRequest* request);
    ClientRequest*      Pop(ReadBuffer key);
    void                FailAll();

private:
    void                OnRemoveCachedWaitQueues();
    void                OnExpireRequests();
    ClientRequest*      Pop(ShardWaitQueue* waitQueue);
    void                Fail(ClientRequest* reqeust);
    ShardWaitQueue*     NewWaitQueue();
    void                DeleteWaitQueue(ShardWaitQueue* waitQueue);
    ShardWaitQueueNode* NewWaitQueueNode();
    void                DeleteWaitQueueNode(ShardWaitQueueNode* waitQueueNode);
    void                UpdateExpireRequestsTimeout();

    unsigned            numWaiting;
    unsigned            waitExpireTime;
    unsigned            maxCacheTime;
    unsigned            maxCacheCount;
    unsigned            maxPoolCount;
    Timer               expireRequests;
    Countdown           removeCachedWaitQueues;
    QueueTree           queueTree;
    QueueCacheList      queueCacheList;
    QueuePoolList       queuePoolList;
    NodeExpiryList      nodeExpiryList;
    NodePoolList        nodePoolList;
    ShardServer*        shardServer;
};

#endif
