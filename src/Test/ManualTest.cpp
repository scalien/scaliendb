#include "Test.h"
#include "Application/Client/SDBPClient.h"
#include "System/Common.h"

using namespace SDBPClient;

TEST_DEFINE(test1);

TEST_DEFINE(TestManualBasic)
{
    if (TEST_SUCCESS != test1())
        printf("%s/n","Test1 failed.");
    else
        printf("%s/n","Test1 succeeded.");

    return TEST_SUCCESS;
}

TEST_DEFINE(test1)
{
    Client          client;
    const char*     nodes[] = {"localhost:7080"};
    ReadBuffer      databaseName = "testdb";
    ReadBuffer      tableName = "testtable3";
    ReadBuffer      key = "a";
    ReadBuffer      value = "b";
    uint64_t        quorumID=1;
    uint64_t        databaseID;
    uint64_t        tableID;

//    client.Init(SIZE(nodes),nodes);
    ClientRequest::NodeList quorumNodes;
    uint64_t nodeID = 100;
    quorumNodes.Append(nodeID);

/*    if (client.CreateDatabase(databaseName) != SDBP_SUCCESS)
        return -1;

    if (client.CreateQuorum(quorumNodes) != SDBP_SUCCESS)
        return -1;
*/

    client.GetDatabaseID(databaseName, databaseID);

    if (client.CreateTable(databaseID, quorumID, tableName) != SDBP_SUCCESS)
        return -1;

    return TEST_SUCCESS;
}