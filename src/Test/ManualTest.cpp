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

TEST_DEFINE(TestManualBasic)
{
    //if (TEST_SUCCESS != test1())
    //    printf("%s\n","Test1 failed.");
    //else
    //    printf("%s\n","Test1 succeeded.");

    if (TEST_SUCCESS != test2())
        printf("%s\n","Test2 failed.");
    else
        printf("%s\n","Test2 succeeded.");

    if (TEST_SUCCESS != test3())
        printf("%s\n","Test3 failed.");
    else
        printf("%s\n","Test3 succeeded.");

    return TEST_SUCCESS;
}

TEST_DEFINE(test1)
{
    Client          client;
    const char*     nodes[] = {"localhost:7080"};
    ReadBuffer      databaseName = "testdb_user5";
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
    ReadBuffer      databaseName = "testdb_randnums_20";
    ReadBuffer      tableName1 = "randnums";
    ReadBuffer      key = "user_id";
    ReadBuffer      initValue = "0";
    ReadBuffer      value;
    uint64_t        number = 0;
    uint64_t        quorumID = 1;
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

    client.Begin();

    for (int i=0; i<10000; ++i)
    {
        rnd = RandomInt(0,1000000);
        tmp.Writef("%U", rnd);
        rbrnd = tmp;

        client.SetIfNotExists(rbrnd,rbpsID);
    }

    client.Submit();

    result = client.GetResult();

    int succ = 0;
    int fail = 0;

    for (result->Begin(); !result->IsEnd(); result->Next())
    {
        (result->CommandStatus() == 0) ? ++succ : ++fail;
    }

    delete result;

    printf("%s%i\t%s%i\n","succeeded sets: ",succ,"failed sets: ",fail);

    return TEST_SUCCESS;

}
TEST_DEFINE(test3)
{

    DCACHE->Init(10000000);
    StorageDatabase db;
    db.Open(".","test_direct");
    StorageTable* table = db.GetTable("randnums");

// Setting random numbers as keys

    uint64_t    rnd;
    uint64_t    psID = 12;
    ReadBuffer  rbpsID;
    ReadBuffer  rbrnd;
    Buffer      tmp;

    tmp.Writef("%U", psID);
    rbpsID = tmp;

    int         succ = 0;
    int         fail = 0;

    SeedRandom();

    for (int i=0; i<10000; ++i)
    {
        rnd = RandomInt(0,1000000);
        tmp.Writef("%U", rnd);
        rbrnd = tmp;

        
        (SetIfNotExists(table, rbrnd, rbpsID)) ? ++succ : ++fail;
    }

    db.Commit();

    printf("%s%i\t%s%i\n","succeeded sets: ",succ,"failed sets: ",fail);

    return TEST_SUCCESS;
}