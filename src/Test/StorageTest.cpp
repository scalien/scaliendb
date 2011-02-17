#include "Test.h"

#include "Framework/Storage/StorageBulkCursor.h"
#include "Framework/Storage/StorageEnvironment.h"
#include "Framework/Storage/StorageAsyncList.h"
#include "System/Events/EventLoop.h"
#include "System/IO/IOProcessor.h"

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
    
    asyncList.startKey = "2";
    asyncList.count = 1000;
    asyncList.offset = 1000;
    asyncList.onComplete = CFunc(OnAsyncListComplete);
    env.AsyncList(4, 1, &asyncList);
    
    while (!asyncListCompleted)
        EventLoop::RunOnce();

    EventLoop::Shutdown();
    IOProcessor::Shutdown();

    env.Close();

    return TEST_SUCCESS;
}