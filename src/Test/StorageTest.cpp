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
    Buffer              dbPath;
    
    dbPath.Write("test/shard/0/db");
    env.Open(dbPath);
    cursor = env.GetBulkCursor(4, 1);
    FOREACH (it, *cursor)
    {
        key = it->GetKey();
        value = it->GetValue();
        TEST_LOG("%.*s => %.*s", key.GetLength(), key.GetBuffer(), value.GetLength(), value.GetBuffer());
    }
    
    return TEST_SUCCESS;
}