#include "Test.h"

#include "Framework/Storage/StorageBulkCursor.h"
#include "Framework/Storage/StorageEnvironment.h"
#include "Framework/Storage/StorageAsyncList.h"
#include "System/Events/EventLoop.h"
#include "System/IO/IOProcessor.h"
#include "System/Stopwatch.h"

TEST_DEFINE(TestStorageBulkCursor)
{
    StorageEnvironment  env;
    StorageBulkCursor*  cursor;
    StorageKeyValue*    it;
    ReadBuffer          key;
    ReadBuffer          value;
    Buffer              prevKey;
    Buffer              dbPath;
    unsigned            i;
    
    dbPath.Write("test/shard/0/db");
    env.Open(dbPath);
    cursor = env.GetBulkCursor(4, 1);
    i = 0;
    FOREACH (it, *cursor)
    {
        key = it->GetKey();
        value = it->GetValue();
        
//        if (ReadBuffer::Cmp(prevKey, key) >= 0)
//        {
//            TEST_LOG("%d", i);
//            TEST_FAIL();
//        }
        if (i > 0 && i % 100000 == 0)
            TEST_LOG("%d", i);
        i++;
        prevKey.Write(key);
//        TEST_LOG("%.*s => %.*s", key.GetLength(), key.GetBuffer(), value.GetLength(), value.GetBuffer());
    }
    
    return TEST_SUCCESS;
}

static StorageAsyncList     asyncList;
static bool                 asyncListCompleted;
static void OnAsyncListComplete()
{
    StorageFileKeyValue*    it;
    ReadBuffer              key;
    ReadBuffer              value;
    
    FOREACH (it, asyncList.lastResult->dataPage)
    {
        key = it->GetKey();
        value = it->GetValue();
        TEST_LOG("%.*s => %.*s", key.GetLength(), key.GetBuffer(), value.GetLength(), value.GetBuffer());
    }
    
    TEST_LOG("OnAsyncListComplete");
    if (asyncList.lastResult->final)
        asyncListCompleted = true;
}

TEST_DEFINE(TestStorageAsyncList)
{
    StorageEnvironment  env;
    Buffer              dbPath;
    
    dbPath.Write("test/shard/0/db");
    env.Open(dbPath);
    
    IOProcessor::Init(1024);
    EventLoop::Init();
    
    asyncListCompleted = false;
    
    asyncList.shardFirstKey.Write("2");
    asyncList.count = 1000;
    asyncList.onComplete = CFunc(OnAsyncListComplete);
    env.AsyncList(4, 1, &asyncList);
    
    while (!asyncListCompleted)
        EventLoop::RunOnce();

    EventLoop::Shutdown();
    IOProcessor::Shutdown();

    env.Close();

    return TEST_SUCCESS;
}

TEST_DEFINE(TestStorageSet)
{
    StorageEnvironment  env;
    Buffer              dbPath;
    Stopwatch           sw;
    Buffer              key;
    Buffer              value;
    ReadBuffer          rbValue;
    long                elapsed;
    bool                ret;
    unsigned            num;
  
    // Initialization ==============================================================================
    IOProcessor::Init(1024);
    EventLoop::Init();
    StartClock();

    dbPath.Write("test/shard/0/db");
    env.Open(dbPath);

    // SET test ====================================================================================
    num = 10*1000;
    value.Allocate(50*1000);
    RandomBuffer(value.GetBuffer(), value.GetLength());
    for (unsigned i = 0; i < num; i++)
    {
        key.Writef("%020u", i);

        sw.Start();
        ret = env.Set(4, 4, key, value);
        if (i % 1000 == 0)
        {
            TEST_LOG("Commiting at %u", i);
            ret &= env.Commit(0);
            EventLoop::RunOnce();
        }
        sw.Stop();
        TEST_ASSERT(ret);
    }
    sw.Start();
    env.Commit(0);
    elapsed = sw.Stop();
    TEST_LOG("%u sets took %ld msec", num, elapsed);

    // GET test ====================================================================================
    num = 10*1000;
    for (unsigned i = 0; i < num; i++)
    {
        key.Writef("%020u", i);

        sw.Start();
        ret = env.Get(4, 4, key, rbValue);
        sw.Stop();
        TEST_ASSERT(ret);
    }
    TEST_LOG("%u gets took %ld msec", num, elapsed);

    // Shutdown ====================================================================================
    EventLoop::Shutdown();
    IOProcessor::Shutdown();
    env.Close();
    
    return TEST_SUCCESS;
}
