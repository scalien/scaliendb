#include "Test.h"
#include "Application/Client/SDBPClient.h"
#include "System/Common.h"

using namespace SDBPClient;

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
    

TEST_DEFINE(TestClientBasic)
{
    Client          client;
    Result*         result;
    const char*     nodes[] = {"localhost:7080"};
    ReadBuffer      databaseName = "testdb";
    ReadBuffer      tableName = "testtable";
    ReadBuffer      key = "hol";
    ReadBuffer      resultKey;
    ReadBuffer      value = "peru";
    ReadBuffer      resultValue;
    int             ret;
    
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
    const char*     nodes[] = {"localhost:7080"};
    ReadBuffer      databaseName = "testdb";
    ReadBuffer      tableName = "testtable";
    ReadBuffer      key;
    ReadBuffer      value;
    char            keybuf[32];
    int             ret;
    unsigned        num = 500000;
        
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

TEST_DEFINE(TestClientBatchedSet2)
{
    Client          client;
    const char*     nodes[] = {"localhost:7080"};
    ReadBuffer      databaseName = "testdb";
    ReadBuffer      tableName = "testtable";
    ReadBuffer      key;
    ReadBuffer      value;
    char            keybuf[32];
    int             ret;
    unsigned        num = 1000000;
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

        if (i % 100000 == 0)
        {
            TEST_LOG("Submitting");
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
    //const char*     nodes[] = {"localhost:7080"};
    const char*     nodes[] = {"192.168.137.51:7080"};
    ReadBuffer      databaseName = "testdb";
    ReadBuffer      tableName = "testtable";
    ReadBuffer      key;
    ReadBuffer      value;
    char            valbuf[128];
    char            keybuf[32];
    int             ret;
    unsigned        num = 10000;
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
    
    TEST_LOG("Generating random data...");
    
    for (unsigned i = 0; i < num; i++)
    {
//        ret = snprintf(keybuf, sizeof(keybuf), "%u", i);
//        key.Wrap(keybuf, ret);
        RandomBuffer(keybuf, sizeof(keybuf));
        key.Wrap(keybuf, sizeof(keybuf));
        RandomBuffer(valbuf, sizeof(valbuf));
        value.Wrap(valbuf, sizeof(valbuf));
        ret = client.Set(key, value);
        if (ret != SDBP_SUCCESS)
            TEST_CLIENT_FAIL();
    }

    TEST_LOG("Generated data, start Submit");

    sw.Start();
    ret = client.Submit();
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    sw.Stop();

    TEST_LOG("elapsed: %ld, req/s = %f", (long) sw.Elapsed(), num / (sw.Elapsed() / 1000.0));

    client.Shutdown();
    
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
    unsigned        num = 500000;
    double          minLatency;
    double          maxLatency;
    double          avgLatency;
    double          latency;
    uint64_t        firstRequestTime;
    uint64_t        firstResponseTime;
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
    for (i = 1, result->Begin(); !result->IsEnd(); i++, result->Next())
    {
        request = result->GetRequestCursor();
        latency = request->responseTime - request->requestTime;
        if (latency < minLatency)
            minLatency = latency;
        if (latency > maxLatency)
            maxLatency = latency;
        avgLatency = ((avgLatency * (i - 1)) + latency) / (double) i;
        if (i == 1)
        {
            firstRequestTime = request->requestTime;
            firstResponseTime = request->responseTime;
        }
        
//        if (request->response.type != CLIENTRESPONSE_VALUE)
//            TEST_CLIENT_FAIL();

        //TEST_LOG("%u: value = %.*s", i, P(&request->response.value));
    }
    TEST_LOG("latency avg, min, max: %lf, %lf, %lf", avgLatency, minLatency, maxLatency);
    TEST_LOG("last-first request time: %" PRIu64, (request->requestTime - firstRequestTime));
    TEST_LOG("last-first response time: %" PRIu64, (request->responseTime - firstResponseTime));

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
    const char*     nodes[] = {"localhost:7080"};
    ReadBuffer      databaseName = "testdb";
    ReadBuffer      tableName = "testtable";
    ReadBuffer      key;
    Buffer          keyBuffer;
    ReadBuffer      resultKey;
    ReadBuffer      value;
    Buffer          valueBuffer;
    ReadBuffer      resultValue;
    int             ret;
    unsigned        i;
    
    Log_SetTimestamping(true);
    Log_SetTarget(LOG_TARGET_STDOUT);
    Log_SetTrace(false);
    
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
    
    client.Begin();
    for (i = 0; i < 10000; i++)
    {
        keyBuffer.Writef("key%u", i);
        key.Wrap(keyBuffer);
        valueBuffer.Writef("value%u", i);
        value.Wrap(valueBuffer);
        ret = client.Set(key, value);
        if (ret != SDBP_SUCCESS)
            TEST_CLIENT_FAIL();
        
//        ret = client.Get(key);
//        if (ret != SDBP_SUCCESS)
//            TEST_CLIENT_FAIL();
//        
//        result = client.GetResult();
//        if (result == NULL)
//            TEST_CLIENT_FAIL();
//        
//        if (result->GetRequestCount() != 1)
//            TEST_CLIENT_FAIL();
//        
//        for (result->Begin(); !result->IsEnd(); result->Next())
//        {
//            ret = result->GetKey(resultKey);
//            if (ret != SDBP_SUCCESS)
//                TEST_CLIENT_FAIL();
//            
//            if (ReadBuffer::Cmp(key, resultKey) != 0)
//                TEST_CLIENT_FAIL();
//            
//            ret = result->GetValue(resultValue);
//            if (ret != SDBP_SUCCESS)
//                TEST_CLIENT_FAIL();
//                
//            if (ReadBuffer::Cmp(value, resultValue) != 0)
//                TEST_CLIENT_FAIL();
//                
//            TEST_LOG("key: %.*s, value: %.*s", P(&resultKey), P(&resultValue));
//        }
//        
//        delete result;
    }

    Stopwatch sw;
    unsigned elapsed;
    sw.Start();
    ret = client.Submit();
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    sw.Stop();
    elapsed = sw.Elapsed();
    TEST_LOG("elapsed %u", elapsed);
    
    result = client.GetResult();
    if (result == NULL)
        TEST_CLIENT_FAIL();

    for (result->Begin(); !result->IsEnd(); result->Next())
    {
        ret = result->TransportStatus();
        if (ret != SDBP_SUCCESS)
            TEST_CLIENT_FAIL();
    }

    delete result;

    client.Shutdown();
    
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
    // TODO: change back
    //const char*     nodes[] = {"localhost:7080"};
    const char*     nodes[] = {"192.168.137.51:7080"};
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
        ret = client.CreateQuorum(quorumNodes);
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

TEST_DEFINE(TestClientFailover)
{
    Client          client;
    Result*         result;
    const char*     nodes[] = {"localhost:7080"};
    ReadBuffer      databaseName = "message_board";
    ReadBuffer      tableName = "messages";
    Buffer          key;
    ReadBuffer      value;
    int             ret;
    
    Log_SetTimestamping(true);
    Log_SetTarget(LOG_TARGET_STDOUT);
    Log_SetTrace(true);
    
    ret = client.Init(SIZE(nodes), nodes);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();

    ret = client.UseDatabase(databaseName);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    ret = client.UseTable(tableName);
    if (ret != SDBP_SUCCESS)
        TEST_CLIENT_FAIL();
    
    while (true)
    {    
        ret = client.Get("index");
        if (ret != SDBP_SUCCESS)
            TEST_CLIENT_FAIL();
            
        result = client.GetResult();
        
        ret = result->GetValue(value);
        if (ret != SDBP_SUCCESS)
            TEST_CLIENT_FAIL();

        TEST_LOG("value = %.*s", value.GetLength(), value.GetBuffer());
        
        delete result;
        
        MSleep(500);
    }
    client.Shutdown();
    
    return TEST_SUCCESS;
}

TEST_MAIN(TestClientBasic);