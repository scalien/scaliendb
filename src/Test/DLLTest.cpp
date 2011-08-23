#include "Test.h"
#include "System/DLL.h"
#include "System/Platform.h"
#include "System/Common.h"
#include "System/Macros.h"

TEST_DEFINE(TestDLLLoad)
{
    DLL         clientLibrary;
    void        *pFunc;
    uint32_t    x;
    
    TEST_ASSERT(clientLibrary.Load("bin/libscaliendbclient"));
    
    pFunc = clientLibrary.GetFunction("NextPowerOfTwo");
    TEST_ASSERT(pFunc);
    
    x = 3;
    x = ((uint32_t(*)(uint32_t)) pFunc)(x);
    TEST_ASSERT(x == 4);
    
    return TEST_SUCCESS;
}
