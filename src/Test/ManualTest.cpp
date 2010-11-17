#include "Test.h"
#include "Application/Client/SDBPClient.h"
#include "System/Common.h"
#include "Framework/Storage/StorageDatabase.h"
#include "Framework/Storage/StorageTable.h"
#include "Framework/Storage/StorageDataCache.h"

using namespace SDBPClient;

bool SetIfNotExists(StorageTable* table, ReadBuffer key, ReadBuffer value)
{
    ReadBuffer v;
    if (table->Get(key,v))
        return false;
    else
        table->Set(key,value);
    return true;
}


TEST_DEFINE(test1);
TEST_DEFINE(test2);
TEST_DEFINE(test3);
TEST_DEFINE(test_rename);
TEST_DEFINE(TestClientDeleteDatabase);

TEST_DEFINE(TestManualBasic)
{

//    TEST_CALL(test1);
    TEST_CALL(test2);
//    TEST_CALL(test3);
//    TEST_CALL(test_rename);
    TEST_CALL(TestClientDeleteDatabase);

    return TEST_SUCCESS;
}

TEST_DEFINE(test1)
{
    Client          client;
    const char*     nodes[] = {"localhost:7080"};
    ReadBuffer      databaseName = "testdb_user";
    ReadBuffer      tableName1 = "user";
    ReadBuffer      tableName2 = "user_name";
    ReadBuffer      tableName3 = "user_meta";
    ReadBuffer      key = "user_id";
    ReadBuffer      initValue = "0";
    ReadBuffer      value;
    uint64_t        number = 0;
    uint64_t        quorumID=1;
    uint64_t        databaseID;
    Result*         result;
    Buffer          tmp;

    client.Init(SIZE(nodes),nodes);
    ClientRequest::NodeList quorumNodes;
    uint64_t nodeID = 100;
    quorumNodes.Append(nodeID);

    if (client.CreateDatabase(databaseName) != SDBP_SUCCESS)
        return -1;

    if (client.CreateQuorum(quorumNodes) != SDBP_SUCCESS)
        return -1;

    client.GetDatabaseID(databaseName, databaseID);

    if (client.CreateTable(databaseID, quorumID, tableName1) != SDBP_SUCCESS)
        return -1;

    if (client.CreateTable(databaseID, quorumID, tableName2) != SDBP_SUCCESS)
        return -1;

    if (client.CreateTable(databaseID, quorumID, tableName3) != SDBP_SUCCESS)
        return -1;

    if (client.UseDatabase(databaseName) != SDBP_SUCCESS)
        return -1;

    if (client.UseTable(tableName3) != SDBP_SUCCESS)
        return -1;

    if (client.Set(key, initValue) != SDBP_SUCCESS)
        return -1;

    // Set

    ReadBuffer      users[5] = {"alma_ata","barack_bela","cseri_csaba","dimi_denes","eke_elek"};

    for (int i=0; i<5; ++i)
    {

        if (client.UseTable(tableName1) != SDBP_SUCCESS)
            return -1;

        tmp.Writef("%U", number);
        if (client.Set(tmp, users[i]) != SDBP_SUCCESS)
            return -1;

        if (client.UseTable(tableName2) != SDBP_SUCCESS)
            return -1;
     
        Buffer  str;
        str.Append(users[i]);
        str.Append(":");
        str.Appendf("%U", number);

        ReadBuffer index = str; 
        ReadBuffer empty = "";

        if (client.Set(str,empty) != SDBP_SUCCESS)
            return -1;

        if (client.UseTable(tableName3) != SDBP_SUCCESS)
            return -1;

        if (client.Add(key, 1) != SDBP_SUCCESS)
            return -1;

        result = client.GetResult();
        result->GetNumber(number);
        delete result;

    }

    return TEST_SUCCESS;
}

TEST_DEFINE(test2)
{
    Client          client;
    const char*     nodes[] = {"localhost:7080"};
    ReadBuffer      databaseName = "testdb_randnums8";
    ReadBuffer      tableName1 = "randnums";
    ReadBuffer      key = "user_id";
    ReadBuffer      initValue = "0";
    ReadBuffer      value;
    uint64_t        quorumID = 1;
    uint64_t        databaseID;
    uint64_t        start;
    uint64_t        end;
    Result*         result;
    Buffer          tmp;
    const int       num = 10000;

    client.Init(SIZE(nodes),nodes);
    ClientRequest::NodeList quorumNodes;
    uint64_t nodeID = 100;
    quorumNodes.Append(nodeID);

    if (client.CreateDatabase(databaseName) != SDBP_SUCCESS)
        return -1;

    if (client.CreateQuorum(quorumNodes) != SDBP_SUCCESS)
        return -1;

    client.GetDatabaseID(databaseName, databaseID);

    if (client.CreateTable(databaseID, quorumID, tableName1) != SDBP_SUCCESS)
        return -1;

    if (client.UseDatabase(databaseName) != SDBP_SUCCESS)
        return -1;

    if (client.UseTable(tableName1) != SDBP_SUCCESS)
        return -1;

// Setting random numbers as keys

    uint64_t    rnd;
    uint64_t    psID = 11;
    ReadBuffer  rbpsID;
    ReadBuffer  rbrnd;

    tmp.Writef("%U", psID);
    rbpsID = tmp;

    SeedRandom();

    start = Now();
    client.Begin();

    for (int i=0; i<num; ++i)
    {
        rnd = RandomInt(0,1000000);
        tmp.Writef("%U", rnd);
        rbrnd = tmp;

        client.SetIfNotExists(rbrnd,rbpsID);
    }

    client.Submit();
    end = Now();

    result = client.GetResult();

    int succ = 0;
    int fail = 0;

    for (result->Begin(); !result->IsEnd(); result->Next())
    {
        (result->CommandStatus() == 0) ? ++succ : ++fail;
    }

    delete result;

    printf("%s%i\t%s%i\n","succeeded sets: ",succ,"failed sets: ",fail);
    printf("elapsed = %lf\n", (double)(num / ((end - start) / 1000.0)));

    return TEST_SUCCESS;

}
TEST_DEFINE(test3)
{
    StorageDatabase     db;
    StorageTable*       table;
    uint64_t            rnd;
    uint64_t            psID = 12;
    uint64_t            start;
    uint64_t            end;
    ReadBuffer          rbpsID;
    ReadBuffer          rbrnd;
    Buffer              tmp;
    const int           num = 10000;

    DCACHE->Init(10000000);
    db.Open(".","test_direct7");
    table = db.GetTable("randnums");

// Setting random numbers as keys

    tmp.Writef("%U", psID);
    rbpsID = tmp;

    int         succ = 0;
    int         fail = 0;

    SeedRandom();

    start = Now();
    for (int i=0; i<num; ++i)
    {
        rnd = RandomInt(0,1000000);
        tmp.Writef("%U", rnd);
        rbrnd = tmp;

        
        (SetIfNotExists(table, rbrnd, rbpsID)) ? ++succ : ++fail;
    }

    db.Commit();

    end = Now();
 
    printf("%s%i\t%s%i\n","succeeded sets: ",succ,"failed sets: ",fail);
    printf("elapsed = %lf\n", (double)(num / ((end - start) / 1000.0)));

    return TEST_SUCCESS;
}
TEST_DEFINE(test_rename)
{
    Client          client;
    const char*     nodes[] = {"localhost:7080"};
    ReadBuffer      databaseName = "test_direct5";
    uint64_t        databaseID;

    client.Init(SIZE(nodes),nodes);
    ClientRequest::NodeList quorumNodes;
    uint64_t nodeID = 100;
    quorumNodes.Append(nodeID);

    //if (client.CreateDatabase(databaseName) != SDBP_SUCCESS)
      //  return TEST_FAILURE;

    client.GetDatabaseID(databaseName, databaseID);

    if (client.RenameDatabase(databaseID, "new_name") != SDBP_SUCCESS)
        return TEST_FAILURE;
        

    return TEST_SUCCESS;
}

TEST_DEFINE(TestClientDeleteDatabase)
{
    Client          client;
    const char*     nodes[] = {"localhost:7080"};
    ReadBuffer      databaseName = "testdb_randnums8";
    uint64_t        databaseID;

    client.Init(SIZE(nodes),nodes);
    ClientRequest::NodeList quorumNodes;
    uint64_t nodeID = 100;
    quorumNodes.Append(nodeID);

    //if (client.CreateDatabase(databaseName) != SDBP_SUCCESS)
      //  return TEST_FAILURE;

    if (client.GetDatabaseID(databaseName, databaseID) != SDBP_SUCCESS)
        return TEST_FAILURE;

    if (client.DeleteDatabase(databaseID) != SDBP_SUCCESS)
        return TEST_FAILURE;
        
    return TEST_SUCCESS;
}
