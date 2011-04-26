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
    
    return TEST_SUCCESS;
}