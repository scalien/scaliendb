#include "Test.h"
#include "Application/Client/SDBPClient.h"

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
    ReadBuffer      databaseName = "mediafilter";
    ReadBuffer      tableName = "users";
    ReadBuffer      key = "hol";
    ReadBuffer      resultKey;
    ReadBuffer      value = "peru";
    ReadBuffer      resultValue;
    int             ret;
    
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
            
        TEST_LOG("key: %.*s, value: %.*s", P(&resultKey), P(&resultValue));
    }
    
    delete result;
    
    client.Shutdown();
    
    return TEST_SUCCESS;
}

TEST_DEFINE(TestClientSet)
{
    Client          client;
    const char*     nodes[] = {"localhost:7080"};
    ReadBuffer      databaseName = "mediafilter";
    ReadBuffer      tableName = "users";
    ReadBuffer      key;
    ReadBuffer      value;
    char            keybuf[32];
    int             ret;
    unsigned        num = 1000000;
    
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
    ReadBuffer      databaseName = "mediafilter";
    ReadBuffer      tableName = "users";
    ReadBuffer      key;
    ReadBuffer      value;
    char            keybuf[32];
    int             ret;
    unsigned        num = 1000;
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

    TEST_LOG("elapsed: %ld, req/s = %f", sw.Elapsed(), num / (sw.Elapsed() / 1000.0));

    client.Shutdown();
    
    return TEST_SUCCESS;
}

TEST_DEFINE(TestClientCreateTable)
{
    typedef ClientRequest::NodeList NodeList;

    Client          client;
    Result*         result;
    const char*     nodes[] = {"localhost:7080"};
    ReadBuffer      databaseName = "mediafilter";
    ReadBuffer      tableName = "users";
    ReadBuffer      key;
    ReadBuffer      value;
    int             ret;
    Stopwatch       sw;
    NodeList        quorumNodes;
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


TEST_MAIN(TestClientBasic);