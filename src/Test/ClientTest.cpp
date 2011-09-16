#include "Test.h"
#include "Application/Client/SDBPClient.h"
#include "Application/Client/SDBPClientWrapper.h"
#include "System/Common.h"
#include "System/Threading/ThreadPool.h"
#include "System/Threading/Atomic.h"

using namespace SDBPClient;

#ifdef TEST
#undef TEST
#endif
#define TEST(x) if (x != SDBP_SUCCESS) TEST_CLIENT_FAIL();

#define TEST_CLIENT_FAIL()                                                      \
    {                                                                           \
        PRINT_CLIENT_STATUS("Transport", client.GetTransportStatus());          \
        PRINT_CLIENT_STATUS("Connectivity", client.GetConnectivityStatus());    \
        PRINT_CLIENT_STATUS("Timeout", client.GetTimeoutStatus());              \
        PRINT_CLIENT_STATUS("Command", client.GetCommandStatus());              \
        if (stopOnTestFailure)                                                  \
            TEST_FAIL();                                                        \
        else                                                                    \
            AtomicIncrement32(failCount);                                       \
    }

#define PRINT_CLIENT_STATUS(which, status) \
    switch (status) \
    { \
    case SDBP_SUCCESS: TEST_LOG("%s status: SDBP_SUCCESS", which); break; \
    case SDBP_API_ERROR: TEST_LOG("%s status: SDBP_API_ERROR", which); break; \
    case SDBP_PARTIAL: TEST_LOG("%s status: SDBP_PARTIAL", which); break; \
    case SDBP_FAILURE: TEST_LOG("%s status: SDBP_FAILURE", which); break; \
    case SDBP_NOMASTER: TEST_LOG("%s status: SDBP_NOMASTER", which); break; \
    case SDBP_NOCONNECTION: TEST_LOG("%s status: SDBP_NOCONNECTION", which); break; \
    case SDBP_NOPRIMARY: TEST_LOG("%s status: SDBP_NOPRIMARY", which); break; \
    case SDBP_MASTER_TIMEOUT: TEST_LOG("%s status: SDBP_MASTER_TIMEOUT", which); break; \
    case SDBP_GLOBAL_TIMEOUT: TEST_LOG("%s status: SDBP_GLOBAL_TIMEOUT", which); break; \
    case SDBP_PRIMARY_TIMEOUT: TEST_LOG("%s status: SDBP_PRIMARY_TIMEOUT", which); break; \
    case SDBP_NOSERVICE: TEST_LOG("%s status: SDBP_NOSERVICE", which); break; \
    case SDBP_FAILED: TEST_LOG("%s status: SDBP_FAILED", which); break; \
    case SDBP_BADSCHEMA: TEST_LOG("%s status: SDBP_BADSCHEMA", which); break; \
    }

// module global variables
static uint64_t     defaultTableID;
static uint64_t     defaultDatabaseID;
static bool         stopOnTestFailure = true;
static uint32_t     failCount;

static int SetupDefaultClient(Client& client)
{
    // XXX: Warning! Don't use localhost on Windows, because if IPv6 is installed
    // resolution of localhost is at least 1 second. Instead use 127.0.0.1
//    const char*     nodes[] = {"192.168.137.50:7080"};
    const char*     nodes[] = {"127.0.0.1:7080"};
//    const char*     nodes[] = {"192.168.137.52:7080"};
//    const char*     nodes[] = {"192.168.1.5:7080"};
    std::string     databaseName = "test";
    std::string     tableName = "test";
    uint64_t        databaseID;
    uint64_t        tableID;
    int             ret;
    ClientObj       clientObj;

    ret = client.Init(SIZE(nodes), nodes);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
   
    client.SetMasterTimeout(10*1000);
    client.SetGlobalTimeout(30*1000);
    //client.SetConsistencyMode(SDBP_CONSISTENCY_RYW);

    clientObj = (ClientObj) &client;
    databaseID = 0;
    for (unsigned i = 0; i < SDBP_GetNumDatabases(clientObj); i++)
    {
        if (SDBP_GetDatabaseNameAt(clientObj, i) == databaseName)
        {
            databaseID = SDBP_GetDatabaseIDAt(clientObj, i);
            break;
        }
    }

    if (databaseID == 0)
        return TEST_FAILURE;
    defaultDatabaseID = databaseID;

    tableID = 0;
    for (unsigned i = 0; i < SDBP_GetNumTables(clientObj, defaultDatabaseID); i++)
    {
        if (SDBP_GetTableNameAt(clientObj, defaultDatabaseID, i) == tableName)
        {
            tableID = SDBP_GetTableIDAt(clientObj, defaultDatabaseID, i);
            break;
        }
    }

    if (tableID == 0)
        return TEST_FAILURE;
    defaultTableID = tableID;

    return TEST_SUCCESS;
}

TEST_DEFINE(TestClientBasic)
{
    Client          client;
    Result*         result;
    ReadBuffer      key = "hol";
    ReadBuffer      resultKey;
    ReadBuffer      value = "peru";
    ReadBuffer      resultValue;
    int             ret;

	ret = SetupDefaultClient(client);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    ret = client.Set(defaultTableID, key, value);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    ret = client.Get(defaultTableID, key);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    result = client.GetResult();
    if (result == NULL)
        TEST_CLIENT_FAIL();
    
    if (result->GetRequestCount() != 1)
        TEST_CLIENT_FAIL();
    
    for (result->Begin(); !result->IsEnd(); result->Next())
    {
        ret = result->GetKey(resultKey);
        if (ret != SDBP_SUCCESS)
            TEST_CLIENT_FAIL();
        
        if (ReadBuffer::Cmp(key, resultKey) != 0)
            TEST_CLIENT_FAIL();
        
        ret = result->GetValue(resultValue);
        if (ret != SDBP_SUCCESS)
            TEST_CLIENT_FAIL();
            
        if (ReadBuffer::Cmp(value, resultValue) != 0)
            TEST_CLIENT_FAIL();
            
        TEST_LOG("key: %.*s, value: %.*s", resultKey.GetLength(), resultKey.GetBuffer(), 
                                            resultValue.GetLength(), resultKey.GetBuffer());
    }
    
    delete result;
    
    client.Shutdown();
    
    return TEST_SUCCESS;
}

TEST_DEFINE(TestClientSet)
{
    Client          client;
    ReadBuffer      key;
    ReadBuffer      value;
    char            keybuf[32];
    int             ret;
    unsigned        num = 1000;
    static unsigned counter;
    unsigned        counterValue;

    IOProcessor::BlockSignals(IOPROCESSOR_BLOCK_ALL);

    ret = SetupDefaultClient(client);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    for (unsigned i = 0; i < num; i++)
    {
        counterValue = AtomicIncrement32(counter);
        ret = snprintf(keybuf, sizeof(keybuf), "%010u", counterValue);
        key.Wrap(keybuf, ret);
        value.Wrap(keybuf, ret);
        ret = client.Set(defaultTableID, key, value);
        if (ret != SDBP_SUCCESS)
        {
            Log_Debug("Something went wrong");
            TEST_CLIENT_FAIL();
        }
    }

	client.Submit();

    client.Shutdown();
    
    return TEST_SUCCESS;
}

TEST_DEFINE(TestClientGet)
{
    Client          client;
    Result*         result;
    ReadBuffer      key;
    char            keybuf[33];
    int             ret;

    ret = SetupDefaultClient(client);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    ret = snprintf(keybuf, sizeof(keybuf), "UzXM3k7UGr");
    key.Wrap(keybuf, ret);
    ret = client.Get(defaultTableID, key);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();

    result = client.GetResult();
    for (result->Begin(); !result->IsEnd(); result->Next())
    {
        if (result->GetCommandStatus() != SDBP_SUCCESS)
            TEST_CLIENT_FAIL();
        TEST_LOG("Elapsed: %u", result->GetElapsedTime());
    }
    
    delete result;

    client.Shutdown();
    
    return TEST_SUCCESS;
}

TEST_DEFINE(TestClientListKeys)
{
    Client          client;
    Result*         result;
    ReadBuffer      key;
    ReadBuffer      endKey;
    ReadBuffer      prefix;
    char            keybuf[33];
    int             ret;
        
    ret = SetupDefaultClient(client);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();

    ret = snprintf(keybuf, sizeof(keybuf), "cfcd208495d565ef66e7dff9f98764da");
//    key.Wrap(keybuf, ret);
    ret = client.ListKeys(defaultTableID, key, endKey, prefix, 3000, false);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();

    result = client.GetResult();
    for (result->Begin(); !result->IsEnd(); result->Next())
    {
        if (result->GetCommandStatus() != SDBP_SUCCESS)
            TEST_CLIENT_FAIL();

        result->GetKey(key);
        TEST_LOG("%.*s", key.GetLength(), key.GetBuffer());
    }
    
    delete result;

    client.Shutdown();
    
    return TEST_SUCCESS;
}

TEST_DEFINE(TestClientListKeysWithPrefix)
{
    Client          client;
    Result*         result;
    ReadBuffer      key;
    int             ret;

    ret = SetupDefaultClient(client);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    ret = client.ListKeys(defaultTableID, "aa1", "", "aa1", 0, 0);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();

    result = client.GetResult();
    for (result->Begin(); !result->IsEnd(); result->Next())
    {
        if (result->GetCommandStatus() != SDBP_SUCCESS)
            TEST_CLIENT_FAIL();

        result->GetKey(key);
        TEST_LOG("%.*s", key.GetLength(), key.GetBuffer());
    }
    
    delete result;

    client.Shutdown();
    
    return TEST_SUCCESS;
}

TEST_DEFINE(TestClientBatchLimit)
{
    Client          client;
    ReadBuffer      key;
    ReadBuffer      value;
    char            keybuf[32];
    int             ret;
    unsigned        num = 1000000000;
    Stopwatch       sw;
        
    ret = SetupDefaultClient(client);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();

    for (unsigned i = 0; i < num; i++)
    {
        ret = snprintf(keybuf, sizeof(keybuf), "%u", i);
        key.Wrap(keybuf, ret);
        value.Wrap(keybuf, ret);
        ret = client.Set(defaultTableID, key, value);
        if (ret != SDBP_SUCCESS)
            TEST_CLIENT_FAIL();
    }

    sw.Start();
    ret = client.Submit();
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    sw.Stop();

    TEST_LOG("elapsed: %ld, req/s = %f", (long) sw.Elapsed(), num / (sw.Elapsed() / 1000.0));

    client.Shutdown();
    
    return TEST_SUCCESS;
}

TEST_DEFINE(TestClientBatchedSet)
{
    Client          client;
    ReadBuffer      key;
    ReadBuffer      value;
    char            keybuf[32];
    int             ret;
    unsigned        num = 100000;
    Stopwatch       sw;
        
    ret = SetupDefaultClient(client);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
   
    for (unsigned i = 0; i < num; i++)
    {
        ret = snprintf(keybuf, sizeof(keybuf), "%010u", i);
        key.Wrap(keybuf, ret);
        value.Wrap(keybuf, ret);
        ret = client.Set(defaultTableID, key, value);
        if (ret != SDBP_SUCCESS)
            TEST_CLIENT_FAIL();
    }

    sw.Start();
    ret = client.Submit();
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    sw.Stop();

    TEST_LOG("elapsed: %ld, req/s = %f", (long) sw.Elapsed(), num / (sw.Elapsed() / 1000.0));

    client.Shutdown();
    
    return TEST_SUCCESS;
}

TEST_DEFINE(TestClientBatchedSet2)
{
    Client          client;
    ReadBuffer      key;
    ReadBuffer      value;
    char            keybuf[32];
//    char            valbuf[100000];
    int             ret;
//    unsigned        num = 100*1000*1000;
    unsigned        num = 100*1000;
    Stopwatch       sw;
        
    ret = SetupDefaultClient(client);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();

    for (unsigned i = 0; i < num; i++)
    {
        ret = snprintf(keybuf, sizeof(keybuf), "%010u", i);
        key.Wrap(keybuf, ret);
        value.Wrap(keybuf, ret);
        ret = client.Set(defaultTableID, key, value);
        if (ret != SDBP_SUCCESS)
            TEST_CLIENT_FAIL();

        if (i != 0 && i % 10000 == 0)
        {
            TEST_LOG("Submitting %d", i);
            sw.Start();
            ret = client.Submit();
            if (ret != SDBP_SUCCESS)
                TEST_CLIENT_FAIL();
            sw.Stop();
            ret = client.Begin();
            if (ret != SDBP_SUCCESS)
                TEST_CLIENT_FAIL();
        }
    }

    sw.Start();
    ret = client.Submit();
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    sw.Stop();

    TEST_LOG("elapsed: %ld, req/s = %f", (long) sw.Elapsed(), num / (sw.Elapsed() / 1000.0));

    client.Shutdown();
    
    return TEST_SUCCESS;
}

TEST_DEFINE(TestClientBatchedSetBulk)
{
    Client          client;
    ReadBuffer      key;
    ReadBuffer      value;
    char            keybuf[32];
    char            valbuf[100];
    int             ret;
//    unsigned        num = 100*1000*1000;
    unsigned        num = 100*1000;
    Stopwatch       sw;
        
    ret = SetupDefaultClient(client);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();

    for (unsigned i = 0; i < num; i++)
    {
        ret = snprintf(keybuf, sizeof(keybuf), "%010u", i);
        key.Wrap(keybuf, ret);
        value.Wrap(valbuf, ret);
        ret = client.Set(defaultTableID, key, value);
        if (ret != SDBP_SUCCESS)
            TEST_CLIENT_FAIL();

        if (i != 0 && i % 10000 == 0)
        {
            TEST_LOG("Submitting %d", i);
            sw.Start();
            ret = client.Submit();
            if (ret != SDBP_SUCCESS)
                TEST_CLIENT_FAIL();
            sw.Stop();
            ret = client.Begin();
            if (ret != SDBP_SUCCESS)
                TEST_CLIENT_FAIL();
        }
    }

    sw.Start();
    ret = client.Submit();
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    sw.Stop();

    TEST_LOG("elapsed: %ld, req/s = %f", (long) sw.Elapsed(), num / (sw.Elapsed() / 1000.0));

    client.Shutdown();
    
    return TEST_SUCCESS;
}

TEST_DEFINE(TestClientBatchedSetRandom)
{
    Client          client;
    ReadBuffer      key;
    ReadBuffer      value;
    char            valbuf[50];
    char            keybuf[10];
    int             ret;
    unsigned        totalNum = 1;
    unsigned        count;
    Stopwatch       sw;
    static int      counter = 0;
    int             id;
    
    id = counter++;
    //TEST_LOG("Started id = %d", id);
    count = 0;

    RandomBuffer(keybuf, sizeof(keybuf));
    RandomBuffer(valbuf, sizeof(valbuf));
    
    ret = SetupDefaultClient(client);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();

    //TEST_LOG("Generating random data...");
    
    while (true)
    {
        sw.Start();
        for (unsigned i = 0; i < totalNum; i++)
        {
            RandomBuffer(keybuf, sizeof(keybuf));
            key.Wrap(keybuf, sizeof(keybuf));
            value.Wrap(valbuf, sizeof(valbuf));
            TEST_ASSERT(defaultTableID != 0);
            ret = client.Set(defaultTableID, key, value);
            if (ret != SDBP_SUCCESS)
            {
                ;
                TEST_CLIENT_FAIL();
                ASSERT_FAIL();
                return TEST_FAILURE;
            }

            count++;
            if (i > 0 && (i % 1000) == 0)
            {
                Log_Debug("XXX %u: i: %u", (unsigned) ThreadPool::GetThreadID(), i);
            }
        }
        ret = client.Submit();
        if (ret != SDBP_SUCCESS)
            TEST_CLIENT_FAIL();
            
        sw.Stop();
        Log_Debug("XXX %u: i: %u", (unsigned) ThreadPool::GetThreadID(), totalNum);

        TEST_LOG("elapsed: %ld, req/s = %f", (long) sw.Elapsed(), count / (sw.Elapsed() / 1000.0));
    }
    client.Shutdown();
    return TEST_SUCCESS;
}

TEST_DEFINE(TestClientBatchedDelete)
{
    Client          client;
    ReadBuffer      key;
    char            keybuf[32];
    int             ret;
    unsigned        num = 10*1000*1000;
    Stopwatch       sw;
        
    ret = SetupDefaultClient(client);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();

    for (unsigned i = 0; i < num; i++)
    {
        ret = snprintf(keybuf, sizeof(keybuf), "%010u", i);
        key.Wrap(keybuf, ret);
        ret = client.Delete(defaultTableID, key);
        if (ret != SDBP_SUCCESS)
            TEST_CLIENT_FAIL();

        if (i != 0 && i % 100000 == 0)
        {
            TEST_LOG("Submitting %d", i);
            sw.Start();
            ret = client.Submit();
            if (ret != SDBP_SUCCESS)
                TEST_CLIENT_FAIL();
            sw.Stop();
            ret = client.Begin();
            if (ret != SDBP_SUCCESS)
                TEST_CLIENT_FAIL();
        }
    }

    sw.Start();
    ret = client.Submit();
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    sw.Stop();

    TEST_LOG("elapsed: %ld, req/s = %f", (long) sw.Elapsed(), num / (sw.Elapsed() / 1000.0));

    client.Shutdown();
    
    return TEST_SUCCESS;
}


TEST_DEFINE(TestClientGetLatency)
{
    Client          client;
    Result*         result;
    Request*        request;
    ReadBuffer      key;
    ReadBuffer      value;
    char            keybuf[32];
    int             ret;
    unsigned        num = 100000;
    double          minLatency;
    double          maxLatency;
    double          avgLatency;
    double          latency;
    Stopwatch       sw;
    unsigned        nread;
        
    ret = SetupDefaultClient(client);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    minLatency = (double)(uint64_t) -1;
    maxLatency = 0;
    avgLatency = 0;
    
    for (unsigned i = 0; i < num; i++)
    {
        ret = snprintf(keybuf, sizeof(keybuf), "%010u", i);
        key.Wrap(keybuf, ret);
        value.Wrap(keybuf, ret);
        ret = client.Get(defaultTableID, key);
        if (ret != SDBP_SUCCESS)
            TEST_CLIENT_FAIL();

        result = client.GetResult();
        if (result == NULL)
            TEST_CLIENT_FAIL();

        request = result->GetRequestCursor();
        
        if (BufferToUInt64(
         request->response.value.GetBuffer(), 
         request->response.value.GetLength(),
         &nread) != i)
            TEST_CLIENT_FAIL();

        latency = request->responseTime - request->requestTime;
        if (latency < minLatency)
            minLatency = latency;
        if (latency > maxLatency)
            maxLatency = latency;
        avgLatency = ((avgLatency * (i - 1)) + latency) / (double) i;
            
        delete result;
    }
    
    TEST_LOG("latency avg, min, max: %lf, %lf, %lf", avgLatency, minLatency, maxLatency);

    client.Shutdown();
    
    return TEST_SUCCESS;
}

TEST_DEFINE(TestClientMaro)
{
    Client          client;
    ReadBuffer      rk, rv;
    Buffer          k, v;
    int             ret;
    Stopwatch       sw;

    ret = SetupDefaultClient(client);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
   
    k.Write("key");
    v.Write("value");
    rk.Wrap(k);
    rv.Wrap(v);

    k.Writef("key");
    v.Writef("value");
    rk.Wrap(k);
    rv.Wrap(v);

    client.Set(defaultTableID, "index", "0");
    
    sw.Start();
    for (int i = 0; i < 10*1000; i++)
    {
        client.Add(defaultTableID, "index", 1);

        k.Writef("%d", i);
        v.Writef("%d", i*i);
        rk.Wrap(k);
        rv.Wrap(v);
        client.Set(defaultTableID, rk, rv);
    }
    sw.Stop();

    Log_Message("Elapsed: %U", sw.Elapsed());
        
    return TEST_SUCCESS;
}

TEST_DEFINE(TestClientAdd)
{
    Client          client;
    Result*         result;
    Buffer          key;
    ReadBuffer      value;
    int             ret;
    unsigned        num = 10;
    
    Log_SetTimestamping(true);
    Log_SetTarget(LOG_TARGET_STDOUT);
    Log_SetTrace(true);
    
    ret = SetupDefaultClient(client);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();

    ret = client.Set(defaultTableID, "user_id", "0");
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    for (unsigned i = 0; i < num; i++)
    {
        ret = client.Add(defaultTableID, "user_id", 1);
        if (ret != SDBP_SUCCESS)
            TEST_CLIENT_FAIL();
    }
    
    ret = client.Get(defaultTableID, "user_id");
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    result = client.GetResult();
    if (result == NULL)
        TEST_CLIENT_FAIL();
    
    result->GetValue(value);
    TEST_LOG("value: %.*s", value.GetLength(), value.GetBuffer());

    client.Shutdown();
    
    return TEST_SUCCESS;
}

TEST_DEFINE(TestClientAddFailover)
{
    Client          client;
    Result*         result;
    ReadBuffer      value;
    uint64_t        number;
    unsigned        i;
    uint64_t        start;
    uint64_t        end;
    int             ret;
    
    TEST(SetupDefaultClient(client));
    ret = client.Set(defaultTableID, "index", "1");
    TEST_ASSERT(ret == SDBP_SUCCESS);

    i = 0;
    while (true)
    {
        i++;
        start = Now();
        TEST(client.Add(defaultTableID, "index", 1));
        end = Now();
        result = client.GetResult();
        
        TEST(result->GetNumber(number));
        TEST_LOG("%u: value = %u, diff = %u", i, (unsigned) number, (unsigned) (end - start));
        
        delete result;
        
        MSleep(500);
    }
    client.Shutdown();
    
    return TEST_SUCCESS;
}

TEST_DEFINE(TestClientSetFailover)
{
    Client          client;
    ReadBuffer      value;
    unsigned        i;
    uint64_t        start;
    uint64_t        end;
    int             ret;
    Buffer          tmp;
    
    TEST(SetupDefaultClient(client));
    ret = client.Set(defaultTableID, "index", "0");
    TEST_ASSERT(ret == SDBP_SUCCESS || ret == SDBP_FAILED);

    i = 0;
    while (true)
    {
        i++;
        tmp.Writef("%d", i);
        start = Now();
        TEST(client.Set(defaultTableID, "index", tmp));
        TEST(client.Submit());
        end = Now();
        TEST_LOG("%i: diff = %u", i, (unsigned) (end - start));
        
        MSleep(500);
    }
    client.Shutdown();
    
    return TEST_SUCCESS;
}


TEST_DEFINE(TestClientListKeyValues)
{
    Client          client;
    Result*         result;
    ReadBuffer      key;
    ReadBuffer      value;
    Buffer          lastKey;
    Buffer          endKey;
    Buffer          prefix;
    unsigned        offset;
    unsigned        num;
    
    TEST(SetupDefaultClient(client));
    
    //prefix.Write("N:0000000000000");
    SeedRandom();
    prefix.Writef("N:%013d", RandomInt(0, RAND_MAX));

    do
    {
        if (lastKey.GetLength() == 0)
            offset = 0;
        else
            offset = 1;

        TEST(client.ListKeyValues(defaultTableID, lastKey, endKey, prefix, 1000, false));
        
        result = client.GetResult();
        num = 0;
        for (result->Begin(); !result->IsEnd(); result->Next())
        {
            num++;
            TEST(result->GetKey(key));
            TEST(result->GetValue(value));
            //if (ReadBuffer::Cmp(key, "1111") > 0 && ReadBuffer::Cmp(key, "11112") < 0)
            //    TEST_LOG("%.*s => %.*s", key.GetLength(), key.GetBuffer(), value.GetLength(), value.GetBuffer());
        }

        lastKey.Write(key);

        delete result;
        if (num == 0)
            break;
    }
    while (true);
    
    return TEST_SUCCESS;
}

TEST_DEFINE(TestClientCount)
{
    Client          client;
    Result*         result;
    uint64_t        number;
    
    TEST(SetupDefaultClient(client));
    TEST(client.Count(defaultTableID, "", "", ""));
    
    result = client.GetResult();
    TEST(result->GetNumber(number));
    
    TEST_LOG("%u", (unsigned) number);
    
    delete result;

    return TEST_SUCCESS;
}

TEST_DEFINE(TestClientMixedReadWriteBatched)
{
    Client          client;
    int             ret;
    
    TEST(SetupDefaultClient(client));
    TEST(client.Begin());

    TEST(client.Get(defaultTableID, "key"));
    ret = client.Set(defaultTableID, "key", "value");
    TEST_ASSERT(ret == SDBP_API_ERROR);

    return TEST_SUCCESS;
}

TEST_DEFINE(TestClientMixedWriteReadBatched)
{
    Client          client;
    int             ret;
    
    TEST(SetupDefaultClient(client));
    TEST(client.Begin());

    TEST(client.Set(defaultTableID, "key", "value"));
    ret = client.Get(defaultTableID, "key");
    TEST_ASSERT(ret == SDBP_API_ERROR);

    return TEST_SUCCESS;
}

TEST_DEFINE(TestClientCreateSchema)
{
    Client          client;
    ClientObj       clientObj;
    const char*     databaseName = "Storage";
    const char*     tableName = "transaction";
    ReadBuffer      rbName;
    unsigned        numDatabases;
    uint64_t        databaseID = 0;
    uint64_t        quorumID = 0;
    
    TEST(SetupDefaultClient(client));
    clientObj = (ClientObj) &client;

    // make sure there is no database under this name
    numDatabases = SDBP_GetNumDatabases(clientObj);
    for (unsigned i = 0; i < numDatabases; i++)
    {
        std::string name = SDBP_GetDatabaseNameAt(clientObj, i);
        if (name == databaseName)
        {
            databaseID = SDBP_GetDatabaseIDAt(clientObj, i);
            break;
        }
    }
    if (databaseID != 0)
        TEST(client.DeleteDatabase(databaseID));

    // create the database with the given name
    rbName.Wrap(databaseName);
    TEST(client.CreateDatabase(rbName));
    numDatabases = SDBP_GetNumDatabases(clientObj);
    for (unsigned i = 0; i < numDatabases; i++)
    {
        std::string name = SDBP_GetDatabaseNameAt(clientObj, i);
        if (name == databaseName)
        {
            databaseID = SDBP_GetDatabaseIDAt(clientObj, i);
            break;
        }
    }
    TEST_ASSERT(databaseID != 0);

    TEST_ASSERT(SDBP_GetNumQuorums(clientObj) > 0);
    quorumID = SDBP_GetQuorumIDAt(clientObj, 0);
    TEST_ASSERT(quorumID != 0);

    rbName.Wrap(tableName);
    TEST(client.CreateTable(databaseID, quorumID, rbName));

    return TEST_SUCCESS;
}

TEST_DEFINE(TestClientDistributeList)
{
    Client          client;
    Result*         result;
    ReadBuffer      key;
    int             count;
    int             num = 100;

    TEST(SetupDefaultClient(client));

    client.SetConsistencyMode(SDBP_CONSISTENCY_ANY);
    for (int i = 0; i < num; i++)
    {
        TEST(client.ListKeys(defaultTableID, "", "", "", 100, false));
        result = client.GetResult();
        count = 0;
        for (result->Begin(); !result->IsEnd(); result->Next())
        {
            TEST(result->GetKey(key));
            count++;
        }
        TEST_LOG("%d", count);
        delete result;
    }

    client.Shutdown();

    return TEST_SUCCESS;
}

TEST_DEFINE(TestClientGetSetGetSetSubmit)
{
    Client          client;
    Result*         result;
    uint64_t        count;

    TEST(SetupDefaultClient(client));

    TEST(client.TruncateTable(defaultTableID));

    TEST_ASSERT(client.Get(defaultTableID, "0000000000001") == SDBP_FAILED);
    TEST(client.Set(defaultTableID, "0000000000001", "test"));
    TEST_ASSERT(client.Get(defaultTableID, "0000000000002") == SDBP_FAILED);
    TEST(client.Set(defaultTableID, "0000000000002", "test"));

    TEST(client.Submit());

    TEST(client.Count(defaultTableID, "", "", ""));
    result = client.GetResult();
    TEST_ASSERT(result != NULL);
    TEST(result->GetNumber(count));
    TEST_ASSERT(count == 2);

    return TEST_SUCCESS;
}

TEST_DEFINE(TestClientTruncateTable)
{
    Result*         result;
    const unsigned  num = 1000;
    uint64_t        table[num];
    char            name[100];
    ReadBuffer      rbName;
    uint64_t        truncateDatabaseID;
    

    //CreateTablesForTruncating:
    {
        Client          client;
        ConfigState     configState;
        ConfigDatabase* configDatabase;

        TEST(SetupDefaultClient(client));
        client.CloneConfigState(configState);
        configDatabase = configState.GetDatabase("TruncateDatabase");
        if (configDatabase != NULL)
        {
            truncateDatabaseID = configDatabase->databaseID;
            uint64_t    *it;
            unsigned    i;
            i = 0;
            FOREACH (it, configDatabase->tables)
            {
                table[i] = *it;
                if (i > SIZE(table))
                    break;
                i++;                
            }
            goto TruncateTables;
            TEST(client.DeleteDatabase(configDatabase->databaseID));
        }

        snprintf(name, sizeof(name), "TruncateDatabase");
        rbName.Wrap(name);
        TEST(client.CreateDatabase(rbName));

        client.CloneConfigState(configState);
        configDatabase = configState.GetDatabase("TruncateDatabase");
        TEST_ASSERT(configDatabase != NULL);
        
        truncateDatabaseID = configDatabase->databaseID;
        for (unsigned i = 0; i < num; i++)
        {
            snprintf(name, sizeof(name), "TestClientTruncate_Table%u", i);
            rbName.Wrap(name);
            TEST(client.CreateTable(truncateDatabaseID, 1, rbName));
            result = client.GetResult();
            TEST(result->GetNumber(table[i]));
            delete result;
        }
    }

    TruncateTables:
    {
        Client  client;
        TEST(SetupDefaultClient(client));

        for (unsigned i = 0; i < num; i++)
        {
            TEST_LOG("i = %d", i);
            TEST(client.TruncateTable(table[i]));
        }
    }

    return TEST_SUCCESS;
}

TEST_DEFINE(TestClientInfiniteLoop)
{
    while (true)
    {
        TEST_ASSERT(TestClientSet() == TEST_SUCCESS);
    }
    
    // this test always fails or gets cancelled
    return TEST_SUCCESS;
}

TEST_DEFINE(TestClientMultiThread)
{
    ThreadPool*     threadPool;
    unsigned        numThread = 1;
    uint32_t        runCount;
    Stopwatch       sw;

    // parallel tests may fail, but that should not affect the main program
    stopOnTestFailure = true;
	
    runCount = 0;

    while(true)
	{
        TEST_LOG("Starting %u threads", numThread);
        sw.Restart();
		threadPool = ThreadPool::Create(numThread);
		threadPool->SetStackSize(256*KiB);

		for (unsigned i = 0; i < numThread; i++)
		{  
            //threadPool->Execute(CFunc((void (*)(void)) TestClientListKeyValues));
	        //threadPool->Execute(CFunc((void (*)(void)) TestClientBatchedGet));
            threadPool->Execute(CFunc((void (*)(void)) TestClientSet));
		}
    
		threadPool->Start();
		threadPool->WaitStop();
	    delete threadPool;
        sw.Stop();
        runCount += numThread;
        TEST_LOG("Elapsed: %ld, fail/run = %u/%u = %2.1f%%", (long) sw.Elapsed(), failCount, runCount, (float)failCount / runCount * 100.0);
	}
    
    return TEST_SUCCESS;
}

TEST_DEFINE(TestClientMultiThreadMulti)
{
    for (int i = 0; i < 10; i++)
        TestClientBatchedSetRandom();
    return TEST_SUCCESS;
}
