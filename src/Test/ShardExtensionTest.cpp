#include "Test.h"
#include "System/Buffers/ReadBuffer.h"
#include "Application/ShardServer/ShardExtension.h"

bool TestShardExtensionFunction(ShardExtensionParam* param)
{
    TEST_LOG("name: %.*s", param->name.length, param->name.buffer);
    return true;
}

TEST_DEFINE(TestShardExtensionBasic)
{
    ShardExtensionRegisterFunction("test", TestShardExtensionFunction);

    return TEST_SUCCESS;
}
