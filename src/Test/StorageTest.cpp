#include "Test.h"

#include "Framework/Storage/StorageBulkCursor.h"
#include "Framework/Storage/StorageEnvironment.h"

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