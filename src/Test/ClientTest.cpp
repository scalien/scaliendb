#include "Test.h"
#include "Application/Client/SDBPClient.h"

using namespace SDBPClient;

TEST_DEFINE(TestClientBasic)
{
    Client      client;
    const char* nodes[] = {"localhost:10000", "localhost:10001", "localhost:10002"};
    
    client.Init(SIZE(nodes), nodes);
    
    return TEST_SUCCESS;
}

TEST_MAIN(TestClientBasic);