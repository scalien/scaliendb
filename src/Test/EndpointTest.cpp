#include "Test.h"
#include "System/IO/Endpoint.h"

TEST_DEFINE(TestEndpointValidity)
{
    TEST_ASSERT(Endpoint::IsValidEndpoint("127.0.0.1:7080") == true);
    TEST_ASSERT(Endpoint::IsValidEndpoint("127.0.0:1:7080") == false);
    TEST_ASSERT(Endpoint::IsValidEndpoint("127.0.0:7080") == false);
    TEST_ASSERT(Endpoint::IsValidEndpoint("127.0:7080") == false);
    TEST_ASSERT(Endpoint::IsValidEndpoint("127.:7080") == false);
    TEST_ASSERT(Endpoint::IsValidEndpoint("127:7080") == false);
    TEST_ASSERT(Endpoint::IsValidEndpoint("127:") == false);
    TEST_ASSERT(Endpoint::IsValidEndpoint("127") == false);
    TEST_ASSERT(Endpoint::IsValidEndpoint("blade6:7080") == true);
    TEST_ASSERT(Endpoint::IsValidEndpoint("blade6") == false);
    TEST_ASSERT(Endpoint::IsValidEndpoint("blade6:") == false);
    TEST_ASSERT(Endpoint::IsValidEndpoint("-blade6:7080") == false);
    TEST_ASSERT(Endpoint::IsValidEndpoint("blade6-:7080") == false);
    TEST_ASSERT(Endpoint::IsValidEndpoint("-blade6-:7080") == false);
    TEST_ASSERT(Endpoint::IsValidEndpoint("fe80:0:0:0:202:b3ff:fe1e:8329:7080") == true);    
    TEST_ASSERT(Endpoint::IsValidEndpoint("::ffff:192.0.2.128:7080") == true);
    TEST_ASSERT(Endpoint::IsValidEndpoint("fe80::202:b3ff:fe1e:8329") == true);    
    
    return TEST_SUCCESS;
}