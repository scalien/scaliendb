#include "Test.h"
#include "Application/Client/SDBPClient.h"
#include "System/Common.h"
#include "System/Threading/ThreadPool.h"

using namespace SDBPClient;

#ifdef TEST
#undef TEST
#endif
#define TEST(x) if (x != SDBP_SUCCESS) TEST_CLIENT_FAIL();

#define TEST_CLIENT_FAIL() \
    { \
        PRINT_CLIENT_STATUS("Transport", client.TransportStatus()); \
        PRINT_CLIENT_STATUS("Connectivity", client.ConnectivityStatus()); \
        PRINT_CLIENT_STATUS("Timeout", client.TimeoutStatus()); \
        PRINT_CLIENT_STATUS("Command", client.CommandStatus()); \
        TEST_FAIL(); \
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
    
static int SetupDefaultClient(Client& client)
{
    const char*     nodes[] = {"localhost:7080"};
//    const char*     nodes[] = {"192.168.137.51:7080"};
//    const char*     nodes[] = {"192.168.1.5:7080"};
    ReadBuffer      databaseName = "bulk";
    ReadBuffer      tableName = "pics";
    int             ret;
        
    ret = client.Init(SIZE(nodes), nodes);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();

    client.SetMasterTimeout(10000);
    client.SetGlobalTimeout(100000);
    
    ret = client.UseDatabase(databaseName);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    ret = client.UseTable(tableName);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
        
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
    
    ret = client.Set(key, value);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    ret = client.Get(key);
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
    unsigned        num = 500000;
        
    ret = SetupDefaultClient(client);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    for (unsigned i = 0; i < num; i++)
    {
        ret = snprintf(keybuf, sizeof(keybuf), "%u", i);
        key.Wrap(keybuf, ret);
        value.Wrap(keybuf, ret);
        ret = client.Set(key, value);
        if (ret != SDBP_SUCCESS)
            TEST_CLIENT_FAIL();
    }

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
    ret = client.Get(key);
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

TEST_DEFINE(TestClientTestAndDelete)
{
    Client          client;
    Result*         result;
    ReadBuffer      key;
    ReadBuffer      value;
    int             ret;
    bool            isConditionalSuccess;

    ret = SetupDefaultClient(client);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    key.Wrap("x");
    value.Wrap("x");
    ret = client.Set(key, value);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    ret = client.TestAndDelete(key, value);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();

    result = client.GetResult();
    for (result->Begin(); !result->IsEnd(); result->Next())
    {
        if (result->GetCommandStatus() != SDBP_SUCCESS)
            TEST_CLIENT_FAIL();
        if (result->IsConditionalSuccess(isConditionalSuccess))
            TEST_CLIENT_FAIL();
        if (!isConditionalSuccess)
            TEST_CLIENT_FAIL();
        TEST_LOG("Elapsed: %u", result->GetElapsedTime());
    }
    
    delete result;

    client.Shutdown();
    
    return TEST_SUCCESS;
}

TEST_DEFINE(TestClientRemove)
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
    ret = client.Remove(key);
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
    const char*     nodes[] = {"localhost:7080"};
    ReadBuffer      databaseName = "testdb";
    ReadBuffer      tableName = "testtable";
    ReadBuffer      key;
    ReadBuffer      endKey;
    ReadBuffer      prefix;
    char            keybuf[33];
    int             ret;
        
    ret = client.Init(SIZE(nodes), nodes);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();

    client.SetMasterTimeout(7000);
    ret = client.UseDatabase(databaseName);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    ret = client.UseTable(tableName);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    ret = snprintf(keybuf, sizeof(keybuf), "cfcd208495d565ef66e7dff9f98764da");
//    key.Wrap(keybuf, ret);
    ret = client.ListKeys(key, endKey, prefix, 3000, 4000);
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
    
    ret = client.ListKeys("aa1", "", "aa1", 0, 0);
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
    const char*     nodes[] = {"localhost:7080"};
    ReadBuffer      databaseName = "testdb";
    ReadBuffer      tableName = "testtable";
    ReadBuffer      key;
    ReadBuffer      value;
    char            keybuf[32];
    int             ret;
    unsigned        num = 1000000000;
    Stopwatch       sw;
        
    ret = client.Init(SIZE(nodes), nodes);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();

    client.SetMasterTimeout(10000);
    ret = client.UseDatabase(databaseName);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    ret = client.UseTable(tableName);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    ret = client.Begin();
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    for (unsigned i = 0; i < num; i++)
    {
        ret = snprintf(keybuf, sizeof(keybuf), "%u", i);
        key.Wrap(keybuf, ret);
        value.Wrap(keybuf, ret);
        ret = client.Set(key, value);
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
    const char*     nodes[] = {"localhost:7080"};
    ReadBuffer      databaseName = "testdb";
    ReadBuffer      tableName = "testtable";
    ReadBuffer      key;
    ReadBuffer      value;
    char            keybuf[32];
    int             ret;
    unsigned        num = 100000;
    Stopwatch       sw;
        
    ret = client.Init(SIZE(nodes), nodes);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();

    client.SetMasterTimeout(10000);
    ret = client.UseDatabase(databaseName);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    ret = client.UseTable(tableName);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    ret = client.Begin();
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    for (unsigned i = 0; i < num; i++)
    {
        ret = snprintf(keybuf, sizeof(keybuf), "%010u", i);
        key.Wrap(keybuf, ret);
        value.Wrap(keybuf, ret);
        ret = client.Set(key, value);
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
    const char*     nodes[] = {"localhost:7080"};
    ReadBuffer      databaseName = "testdb";
    ReadBuffer      tableName = "testtable";
    ReadBuffer      key;
    ReadBuffer      value;
    char            keybuf[32];
//    char            valbuf[100000];
    int             ret;
//    unsigned        num = 100*1000*1000;
    unsigned        num = 100*1000;
    Stopwatch       sw;
        
    ret = client.Init(SIZE(nodes), nodes);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();

    client.SetMasterTimeout(100000);
    client.SetGlobalTimeout(100000);
    ret = client.UseDatabase(databaseName);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    ret = client.UseTable(tableName);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    ret = client.Begin();
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    for (unsigned i = 0; i < num; i++)
    {
        ret = snprintf(keybuf, sizeof(keybuf), "%010u", i);
        key.Wrap(keybuf, ret);
        value.Wrap(keybuf, ret);
        ret = client.Set(key, value);
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
    const char*     nodes[] = {"localhost:7080"};
    ReadBuffer      databaseName = "testdb";
    ReadBuffer      tableName = "testtable";
    ReadBuffer      key;
    ReadBuffer      value;
    char            keybuf[32];
    char            valbuf[100];
    int             ret;
//    unsigned        num = 100*1000*1000;
    unsigned        num = 100*1000;
    Stopwatch       sw;
        
    ret = client.Init(SIZE(nodes), nodes);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();

    client.SetBulkLoading(true);
    client.SetMasterTimeout(100000);
    client.SetGlobalTimeout(100000);
    ret = client.UseDatabase(databaseName);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    ret = client.UseTable(tableName);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    ret = client.Begin();
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    for (unsigned i = 0; i < num; i++)
    {
        ret = snprintf(keybuf, sizeof(keybuf), "%010u", i);
        key.Wrap(keybuf, ret);
        value.Wrap(valbuf, ret);
        ret = client.Set(key, value);
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
    const char*     nodes[] = {"localhost:7080"};
    //const char*     nodes[] = {"192.168.137.51:7080"};
    ReadBuffer      databaseName = "testdb";
    ReadBuffer      tableName = "testtable";
    ReadBuffer      key;
    ReadBuffer      value;
    char            valbuf[50];
    char            keybuf[10];
    int             ret;
    unsigned        totalNum = 10000;
    unsigned        batchNum = 5000;
    unsigned        count;
    Stopwatch       sw;
    static int      counter = 0;
    int             id;
    
    id = counter++;
    //TEST_LOG("Started id = %d", id);
    count = 0;

    RandomBuffer(keybuf, sizeof(keybuf));
    RandomBuffer(valbuf, sizeof(valbuf));
    
    ret = client.Init(SIZE(nodes), nodes);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();

    client.SetMasterTimeout(10000);
    ret = client.UseDatabase(databaseName);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    ret = client.UseTable(tableName);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();

    for (unsigned x = 0; x < totalNum / batchNum; x++)
    {
        ret = client.Begin();
        if (ret != SDBP_SUCCESS)
            TEST_CLIENT_FAIL();
        
        //TEST_LOG("Generating random data...");
        
        for (unsigned i = 0; i < batchNum; i++)
        {
    //        ret = snprintf(keybuf, sizeof(keybuf), "%u", i);
    //        key.Wrap(keybuf, ret);
//            RandomBuffer(keybuf, sizeof(keybuf));
            key.Wrap(keybuf, sizeof(keybuf));
//            RandomBuffer(valbuf, sizeof(valbuf));
            value.Wrap(valbuf, sizeof(valbuf));
            ret = client.Set(key, value);
            count++;
            if (ret != SDBP_SUCCESS)
                TEST_CLIENT_FAIL();
        }

        //TEST_LOG("Generated data, start Submit");
        //Log_Message("start id = %d", id);

        sw.Start();
        ret = client.Submit();
        if (ret != SDBP_SUCCESS)
            TEST_CLIENT_FAIL();
        sw.Stop();

        //Log_Message("stop id = %d", id);

        if ((batchNum / (sw.Elapsed() / 1000.0)) > 100*1000)
        {
            PRINT_CLIENT_STATUS("Transport", client.TransportStatus());
            PRINT_CLIENT_STATUS("Connectivity", client.ConnectivityStatus());
            PRINT_CLIENT_STATUS("Timeout", client.TimeoutStatus());
            PRINT_CLIENT_STATUS(" Command", client.CommandStatus());
        }
        
    }

    TEST_LOG("elapsed: %ld, req/s = %f", (long) sw.Elapsed(), count / (sw.Elapsed() / 1000.0));
    
    client.Shutdown();
    return TEST_SUCCESS;
}

TEST_DEFINE(TestClientBatchedDelete)
{
    Client          client;
    const char*     nodes[] = {"localhost:7080"};
    ReadBuffer      databaseName = "testdb";
    ReadBuffer      tableName = "testtable";
    ReadBuffer      key;
    char            keybuf[32];
    int             ret;
    unsigned        num = 10*1000*1000;
    Stopwatch       sw;
        
    ret = client.Init(SIZE(nodes), nodes);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();

    client.SetMasterTimeout(10000);
    ret = client.UseDatabase(databaseName);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    ret = client.UseTable(tableName);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    ret = client.Begin();
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    for (unsigned i = 0; i < num; i++)
    {
        ret = snprintf(keybuf, sizeof(keybuf), "%010u", i);
        key.Wrap(keybuf, ret);
        ret = client.Delete(key);
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

TEST_DEFINE(TestClientBatchedDummy)
{
    Client          client;
    const char*     nodes[] = {"localhost:7080"};
    int             ret;

    ret = client.Init(SIZE(nodes), nodes);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();

    ret = client.Begin();
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();

    ret = client.Submit();
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
        
    return TEST_SUCCESS;
}

TEST_DEFINE(TestClientBatchedGet)
{
    Client          client;
    Result*         result;
    Request*        request;
    const char*     nodes[] = {"localhost:7080"};
    ReadBuffer      databaseName = "testdb";
    ReadBuffer      tableName = "testtable";
    ReadBuffer      key;
    ReadBuffer      value;
    char            keybuf[32];
    int             ret;
    unsigned        num = 100000;
    double          minLatency;
    double          maxLatency;
    double          avgLatency;
    double          latency;
    uint64_t        firstRequestTime;
    uint64_t        firstResponseTime;
    Stopwatch       sw;
    unsigned        nread;
        
    ret = client.Init(SIZE(nodes), nodes);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();

    client.SetMasterTimeout(120000);
    client.SetConsistencyLevel(SDBP_CONSISTENCY_ANY);
    ret = client.UseDatabase(databaseName);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    ret = client.UseTable(tableName);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    ret = client.Begin();
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    for (unsigned i = 0; i < num; i++)
    {
        ret = snprintf(keybuf, sizeof(keybuf), "%010u", i);
        key.Wrap(keybuf, ret);
        value.Wrap(keybuf, ret);
        ret = client.Get(key);
        if (ret != SDBP_SUCCESS)
            TEST_CLIENT_FAIL();
    }

    TEST_LOG("Sending requests");

    sw.Start();
    ret = client.Submit();
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    sw.Stop();

    TEST_LOG("elapsed: %ld, req/s = %f", (long) sw.Elapsed(), num / (sw.Elapsed() / 1000.0));

    result = client.GetResult();
    if (result == NULL)
        TEST_CLIENT_FAIL();
    
    minLatency = (double)(uint64_t) -1;
    maxLatency = 0;
    avgLatency = 0;
    unsigned i;
    for (i = 0, result->Begin(); !result->IsEnd(); i++, result->Next())
    {
        request = result->GetRequestCursor();
        latency = request->responseTime - request->requestTime;
        if (latency < minLatency)
            minLatency = latency;
        if (latency > maxLatency)
            maxLatency = latency;
        if (i == 0)
            avgLatency = latency;
        else
            avgLatency = ((avgLatency * (i - 1)) + latency) / (double) i;
        if (i == 0)
        {
            firstRequestTime = request->requestTime;
            firstResponseTime = request->responseTime;
        }
        
        if (request->response.type != CLIENTRESPONSE_VALUE)
            TEST_CLIENT_FAIL();
        
        if (BufferToUInt64(
         request->response.value.GetBuffer(), 
         request->response.value.GetLength(),
         &nread) != i)
            TEST_CLIENT_FAIL();
        

        //TEST_LOG("%u: value = %.*s", i, P(&request->response.value));
    }
    TEST_LOG("latency avg, min, max: %lf, %lf, %lf", avgLatency, minLatency, maxLatency);
    TEST_LOG("last-first request time: %" PRIu64, (request->requestTime - firstRequestTime));
    TEST_LOG("last-first response time: %" PRIu64, (request->responseTime - firstResponseTime));

    client.Shutdown();
    
    return TEST_SUCCESS;
}

TEST_DEFINE(TestClientBatchedGet2)
{
    Client          client;
    const char*     nodes[] = {"localhost:7080"};
    ReadBuffer      databaseName = "testdb";
    ReadBuffer      tableName = "testtable";
    ReadBuffer      key;
    ReadBuffer      value;
    char            keybuf[32];
    int             ret;
    unsigned        num = 10*1000;
    unsigned        batch = 100;
    Stopwatch       sw;
        
    ret = client.Init(SIZE(nodes), nodes);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();

    client.SetMasterTimeout(10000);
    client.SetConsistencyLevel(SDBP_CONSISTENCY_RYW);
    ret = client.UseDatabase(databaseName);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    ret = client.UseTable(tableName);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    for (unsigned i = 0; i < num / batch; i++)
    {
        ret = client.Begin();
        if (ret != SDBP_SUCCESS)
            TEST_CLIENT_FAIL();

        for (unsigned j = 0; j < batch; j++)
        {
            ret = snprintf(keybuf, sizeof(keybuf), "%010u", i * batch + j);
            key.Wrap(keybuf, ret);
            value.Wrap(keybuf, ret);
            ret = client.Get(key);
            if (ret != SDBP_SUCCESS)
                TEST_CLIENT_FAIL();
        }
        TEST_LOG("Sending requests");

        sw.Restart();
        ret = client.Submit();
        if (ret != SDBP_SUCCESS)
            TEST_CLIENT_FAIL();
        sw.Stop();

        TEST_LOG("elapsed: %ld, req/s = %f", (long) sw.Elapsed(), batch / (sw.Elapsed() / 1000.0));
    }

    client.Shutdown();
    
    return TEST_SUCCESS;
}

TEST_DEFINE(TestClientGetLatency)
{
    Client          client;
    Result*         result;
    Request*        request;
    const char*     nodes[] = {"localhost:7080"};
    ReadBuffer      databaseName = "testdb";
    ReadBuffer      tableName = "testtable";
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
        
    ret = client.Init(SIZE(nodes), nodes);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();

    client.SetMasterTimeout(10000);
    ret = client.UseDatabase(databaseName);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    ret = client.UseTable(tableName);
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
        ret = client.Get(key);
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

TEST_DEFINE(TestClientCreateTable)
{
    Client          client;
    Result*         result;
    const char*     nodes[] = {"localhost:7080"};
    ReadBuffer      databaseName = "mediafilter";
    ReadBuffer      tableName = "users";
    ReadBuffer      key;
    ReadBuffer      value;
    int             ret;
    Stopwatch       sw;
    List<uint64_t>  quorumNodes;
    uint64_t        quorumID;
    uint64_t        databaseID;
//    uint64_t        tableID;
    uint64_t        defaultQuorumNodeID = 100;
        
    ret = client.Init(SIZE(nodes), nodes);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();

    client.SetMasterTimeout(10000);
    
    // quorum
    quorumNodes.Append(defaultQuorumNodeID);
    ret = client.CreateQuorum("", quorumNodes);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    result = client.GetResult();
    if (result == NULL)
        TEST_CLIENT_FAIL();
    
    ret = result->GetNumber(quorumID);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    delete result;
    
    // database
    ret = client.CreateDatabase(databaseName);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();

    result = client.GetResult();
    if (result == NULL)
        TEST_CLIENT_FAIL();
    
    ret = result->GetNumber(databaseID);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();

    delete result;
    
    // table
    ret = client.CreateTable(databaseID, quorumID, tableName);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    client.Shutdown();
    
    return TEST_SUCCESS;
}

TEST_DEFINE(TestClientGetAndSet)
{
    Client          client;
//    Result*         result;
    const char*     nodes[] = {"localhost:7080"};
    ReadBuffer      databaseName = "test";
    ReadBuffer      tableName = "tabla";
    ReadBuffer      key;
    ReadBuffer      value;
    ReadBuffer      newVal;
    ReadBuffer      retBuf;
    int             ret;
    Stopwatch       sw;
    List<uint64_t>  quorumNodes;
//    uint64_t        quorumID;
//    uint64_t        databaseID;
//    uint64_t        tableID;
//    uint64_t        defaultQuorumNodeID = 100;
    unsigned        num = 25;
/*        
    ret = client.Init(SIZE(nodes), nodes);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();

    client.SetMasterTimeout(10000);
    
    // quorum
    quorumNodes.Append(defaultQuorumNodeID);
    ret = client.CreateQuorum(quorumNodes);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    result = client.GetResult();
    if (result == NULL)
        TEST_CLIENT_FAIL();
    
    ret = result->GetNumber(quorumID);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    delete result;
    
    // database
    ret = client.CreateDatabase(databaseName);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();

    result = client.GetResult();
    if (result == NULL)
        TEST_CLIENT_FAIL();
    
    ret = result->GetNumber(databaseID);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();

    delete result;
    
    // table
    ret = client.CreateTable(databaseID, quorumID, tableName);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    */
    //set & getandset

   ret = client.Init(SIZE(nodes), nodes);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();

    client.SetMasterTimeout(1000);
    ret = client.UseDatabase(databaseName);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    ret = client.UseTable(tableName);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();

    key.SetBuffer((char*) "1234567890123456789012345");
    value.SetBuffer((char*) "1234567890123456789012345");
    newVal.SetBuffer((char*) "abcdefghijabcdefghijabcde");

    for (unsigned i = 0; i < num; i++)
    {
        key.SetLength(i+1);
        value.SetLength(i+1);

        ret = client.Set(key, value);
        if (ret != SDBP_SUCCESS)
            TEST_CLIENT_FAIL();

        newVal.SetLength(i+1);
        
        ret = client.GetAndSet(key, newVal);
        if (ret != SDBP_SUCCESS)
            TEST_CLIENT_FAIL();
    }

    client.Shutdown();
    
    return TEST_SUCCESS;

}

TEST_DEFINE(TestClientMaro)
{
    Client          client;
    Result*         result;
    ReadBuffer      rk, rv, r;
    Buffer          k, v;
    int             ret;

    ret = SetupDefaultClient(client);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();

    

    for (int i = 0; i < 11; i++)
    {
        k.Writef("%d", i);
        v.Writef("%d", i*i);
        rk.Wrap(k);
        rv.Wrap(v);
        client.Set(rk, rv);

        result = client.GetResult();
        result->Begin();
        result->GetValue(r);
        delete result;
        
    }
    client.Submit();
    
    return TEST_SUCCESS;
}

TEST_DEFINE(TestClientAdd)
{
    Client          client;
    Result*         result;
    const char*     nodes[] = {"localhost:7080"};
    ReadBuffer      databaseName = "testdb";
    ReadBuffer      tableName = "testtable";
    Buffer          key;
    ReadBuffer      value;
    int             ret;
    unsigned        num = 10;
    
    Log_SetTimestamping(true);
    Log_SetTarget(LOG_TARGET_STDOUT);
    Log_SetTrace(true);
    
    ret = client.Init(SIZE(nodes), nodes);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();

    client.SetMasterTimeout(1000);
    ret = client.UseDatabase(databaseName);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    ret = client.UseTable(tableName);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    ret = client.Set("user_id", "0");
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    for (unsigned i = 0; i < num; i++)
    {
        ret = client.Add("user_id", 1);
        if (ret != SDBP_SUCCESS)
            TEST_CLIENT_FAIL();
    }
    
    ret = client.Get("user_id");
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

TEST_DEFINE(TestClientAppend)
{
    Client          client;
    Result*         result;
    const char*     nodes[] = {"localhost:7080"};
    ReadBuffer      databaseName = "testdb";
    ReadBuffer      tableName = "testtable";
    Buffer          key;
    ReadBuffer      value;
    int             ret;
    unsigned        num = 10;
    Stopwatch       sw;
    List<uint64_t>  quorumNodes;
//    uint64_t        quorumID;
//    uint64_t        databaseID;
//    uint64_t        defaultQuorumNodeID = 100;

    Log_SetTimestamping(true);
    Log_SetTarget(LOG_TARGET_STDOUT);
    Log_SetTrace(true);
 
    ret = client.Init(SIZE(nodes), nodes);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();

    client.SetMasterTimeout(1000);

    //// quorum
    //quorumNodes.Append(defaultQuorumNodeID);
    //ret = client.CreateQuorum(quorumNodes);
    //if (ret != SDBP_SUCCESS)
    //    TEST_CLIENT_FAIL();
    //
    //result = client.GetResult();
    //if (result == NULL)
    //    TEST_CLIENT_FAIL();
    //
    //ret = result->GetNumber(quorumID);
    //if (ret != SDBP_SUCCESS)
    //    TEST_CLIENT_FAIL();
    //
    //delete result;

    // database
    //ret = client.CreateDatabase(databaseName);
    //if (ret != SDBP_SUCCESS)
    //    TEST_CLIENT_FAIL();

    //result = client.GetResult();
    //if (result == NULL)
    //    TEST_CLIENT_FAIL();
    //
    //ret = result->GetNumber(databaseID);
    //if (ret != SDBP_SUCCESS)
    //    TEST_CLIENT_FAIL();

    //delete result;
    
    // table
    //ret = client.CreateTable(databaseID, quorumID, tableName);
    //if (ret != SDBP_SUCCESS)
    //    TEST_CLIENT_FAIL();


    // set and append

     ret = client.UseDatabase(databaseName);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    ret = client.UseTable(tableName);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    ret = client.Set("user2_id", "0");
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    for (unsigned i = 0; i < num; i++)
    {
        ret = client.Append("user2_id", "a");
        if (ret != SDBP_SUCCESS)
            TEST_CLIENT_FAIL();
    }
    
    ret = client.Get("user2_id");
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

TEST_DEFINE(TestClientSchemaSet)
{
    Client          client;
    Result*         result;
    const char*     nodes[] = {"localhost:7080"};
    //const char*     nodes[] = {"192.168.137.51:7080"};
    ReadBuffer      databaseName = "testdb";
    ReadBuffer      tableName = "testtable";
    Buffer          key;
    ReadBuffer      value;
    int             ret;
    uint64_t        quorumID;
    uint64_t        databaseID;
    uint64_t        tableID;
    
    ret = client.Init(SIZE(nodes), nodes);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();

  
    ret = client.GetDatabaseID(databaseName, databaseID);
    if (ret != SDBP_SUCCESS && ret != SDBP_BADSCHEMA)
        TEST_CLIENT_FAIL();

    if (ret == SDBP_BADSCHEMA)
    {
        ret = client.CreateDatabase(databaseName);
        if (ret != SDBP_SUCCESS)
            TEST_CLIENT_FAIL();

        result = client.GetResult();
        ret = result->GetNumber(databaseID);
        if (ret != SDBP_SUCCESS)
            TEST_CLIENT_FAIL();
        
        delete result;

        List<uint64_t> quorumNodes;
        uint64_t nodeID = 100;
        quorumNodes.Append(nodeID);
        ret = client.CreateQuorum("", quorumNodes);
        if (ret != SDBP_SUCCESS)
            TEST_CLIENT_FAIL();
        
        result = client.GetResult();
        ret = result->GetNumber(quorumID);
        if (ret != SDBP_SUCCESS)
            TEST_CLIENT_FAIL();
        
        delete result;
    }
    else
        quorumID = 1;   // TODO:

    ret = client.GetTableID(tableName, databaseID, tableID);
    if (ret != SDBP_SUCCESS && ret != SDBP_BADSCHEMA)
        TEST_CLIENT_FAIL();

    if (ret == SDBP_BADSCHEMA)
    {
        ret = client.CreateTable(databaseID, quorumID, tableName);
        if (ret != SDBP_SUCCESS)
            TEST_CLIENT_FAIL();
    }
    
    ret = client.UseDatabase(databaseName);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    ret = client.UseTable(tableName);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    //ret = client.Set("user_id", "0");
    //if (ret != SDBP_SUCCESS)
    //    TEST_CLIENT_FAIL();
    
    client.Shutdown();
    
    return TEST_SUCCESS;
}

TEST_DEFINE(TestClientGetFailover)
{
    Client          client;
    Result*         result;
    ReadBuffer      value;
    unsigned        i;
    uint64_t        start;
    uint64_t        end;
    int             ret;
    
    TEST(SetupDefaultClient(client));
    ret = client.SetIfNotExists("index", "1");
    TEST_ASSERT(ret == SDBP_SUCCESS || ret == SDBP_FAILED);

    i = 0;
    while (true)
    {
        i++;
        start = Now();
        TEST(client.Get("index"));
        end = Now();
        result = client.GetResult();
        
        TEST(result->GetValue(value));
        TEST_LOG("%u: value = %.*s, diff = %u", i, value.GetLength(), value.GetBuffer(), (unsigned) (end - start));
        
        delete result;
        
        MSleep(500);
    }
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
    ret = client.SetIfNotExists("index", "1");
    TEST_ASSERT(ret == SDBP_SUCCESS || ret == SDBP_FAILED);

    i = 0;
    while (true)
    {
        i++;
        start = Now();
        TEST(client.Add("index", 1));
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
    ret = client.Set("index", "0");
    TEST_ASSERT(ret == SDBP_SUCCESS || ret == SDBP_FAILED);

    i = 0;
    while (true)
    {
        i++;
        tmp.Writef("%d", i);
        start = Now();
        TEST(client.Set("index", tmp));
        end = Now();
        TEST_LOG("%i: diff = %u", i, (unsigned) (end - start));
        
        MSleep(500);
    }
    client.Shutdown();
    
    return TEST_SUCCESS;
}

TEST_DEFINE(TestClientMultiThread)
{
    ThreadPool*     threadPool;
    unsigned        numThread = 10;
    
    threadPool = ThreadPool::Create(numThread);
    
    for (unsigned i = 0; i < numThread; i++)
    {  
//        threadPool->Execute(CFunc((void (*)(void)) TestClientBatchedSetRandom));
        threadPool->Execute(CFunc((void (*)(void)) TestClientBatchedGet));
    }
    
    threadPool->Start();
    threadPool->WaitStop();
    
    delete threadPool;
    
    return TEST_SUCCESS;
}

TEST_DEFINE(TestClientMultiThreadMulti)
{
    for (int i = 0; i < 10; i++)
        TestClientBatchedSetRandom();
    return TEST_SUCCESS;
}

TEST_DEFINE(TestClientActivateNode)
{
    Client          client;
    const char*     nodes[] = {"localhost:7080"};
    int             ret;

    ret = client.Init(SIZE(nodes), nodes);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();

    ret = client.ActivateNode(101);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    return TEST_SUCCESS;
}

TEST_DEFINE(TestClientFilter)
{
    uint64_t        commandID;
    Client          client;
    Result*         result;
    ReadBuffer      key;
    ReadBuffer      value;
    
    TEST(SetupDefaultClient(client));
    
    // filter through all key-values in the database
    TEST(client.Filter("", "", "", 1000*1000*1000, 0, commandID));

    do
    {
        result = client.GetResult();
        for (result->Begin(); !result->IsEnd(); result->Next())
        {
            TEST(result->GetKey(key));
            TEST(result->GetValue(value));
            if (ReadBuffer::Cmp(value, "1111") > 0 && ReadBuffer::Cmp(value, "11112") < 0)
                TEST_LOG("%.*s => %.*s", key.GetLength(), key.GetBuffer(), value.GetLength(), value.GetBuffer());
        }

        if (result->IsFinished())
        {
            delete result;
            break;
        }
        
        delete result;
        TEST(client.Receive(commandID));
    }
    while (true);
    
    return TEST_SUCCESS;
}

// emulate Filter with ListKeyValues
TEST_DEFINE(TestClientFilter2)
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
    
    // filter through all key-values in the database
    do
    {
        if (lastKey.GetLength() == 0)
            offset = 0;
        else
            offset = 1;

        TEST(client.ListKeys(lastKey, endKey, prefix, 1000, offset));
        
        result = client.GetResult();
        num = 0;
        for (result->Begin(); !result->IsEnd(); result->Next())
        {
            num++;
            TEST(result->GetKey(key));
//            TEST(result->GetValue(value));
            if (ReadBuffer::Cmp(key, "1111") > 0 && ReadBuffer::Cmp(key, "11112") < 0)
                TEST_LOG("%.*s => %.*s", key.GetLength(), key.GetBuffer(), value.GetLength(), value.GetBuffer());
        }

        delete result;
        if (num == 0)
            break;

        lastKey.Write(key);
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
    TEST(client.Count("", "", "", 0, 0));
    
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

    TEST(client.Get("key"));
    ret = client.Set("key", "value");
    TEST_ASSERT(ret == SDBP_API_ERROR);

    return TEST_SUCCESS;
}

TEST_DEFINE(TestClientMixedWriteReadBatched)
{
    Client          client;
    int             ret;
    
    TEST(SetupDefaultClient(client));
    TEST(client.Begin());

    TEST(client.Set("key", "value"));
    ret = client.Get("key");
    TEST_ASSERT(ret == SDBP_API_ERROR);

    return TEST_SUCCESS;
}

TEST_DEFINE(TestClientTestAndSet)
{
    Client          client;
    
    TEST(SetupDefaultClient(client));

    // make sure there is a value
    TEST(client.Set("key", "value"));
    TEST(client.TestAndSet("key", "value", "value2"));
    TEST(client.TestAndSet("key", "value", "value2"));
    
    return TEST_SUCCESS;
}
