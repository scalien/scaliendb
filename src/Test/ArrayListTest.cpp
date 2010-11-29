#include "Test.h"
#include "System/Containers/ArrayList.h"
#include "System/Platform.h"

TEST_DEFINE(TestArrayListBasic)
{
    ArrayList<uint64_t, 7>  nodes;
    uint64_t*               it;
    uint64_t                nodeID;
    uint64_t                i;
    
    for (i = 0; i <= nodes.GetSize(); i++)
    {
        if (i < nodes.GetSize())
            TEST_ASSERT(nodes.Append(i));
        else
            TEST_ASSERT(!nodes.Append(i));
    }

    for (i = 0, it = nodes.First(); it != NULL; i++, it = nodes.Next(it))
    {
        nodeID = *it;
        TEST_ASSERT(nodeID == i);
    }
    
    return TEST_SUCCESS;
}

TEST_DEFINE(TestArrayListRemove)
{
    ArrayList<uint64_t, 7>  nodes;
    uint64_t*               it;
    uint64_t                nodeID;
    uint64_t                i;
    
    for (i = 0; i <= nodes.GetSize(); i++)
    {
        if (i < nodes.GetSize())
            TEST_ASSERT(nodes.Append(i));
        else
            TEST_ASSERT(!nodes.Append(i));
    }

    for (i = 0, it = nodes.First(); it != NULL; i++, it = nodes.Remove(it))
    {
        nodeID = *it;
        TEST_ASSERT(nodeID == i);
        TEST_ASSERT(nodes.GetLength() == nodes.GetSize() - i);
    }
    
    TEST_ASSERT(nodes.GetLength() == 0);
    
    return TEST_SUCCESS;
}


TEST_MAIN(TestArrayListBasic, TestArrayListRemove);