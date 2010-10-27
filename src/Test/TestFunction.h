#ifndef TESTFUNCTION_H
#define TESTFUNCTION_H

#include "Test.h"
#include "System/Containers/InList.h"
#include "System/Time.h"

#define TEST_MAKENAME(f) #f
#define TEST_CONCAT2(a, b) a ## b
#define TEST_CONCAT(a, b) TEST_CONCAT2(a, b)

class TestFunction
{
public:
    TestFunction(testfn_t function_, const char* name_) : function(function_), name(name_) { prev = next = this; }
    
    testfn_t        function;
    const char*     name;
    TestFunction*   next;
    TestFunction*   prev;    
};

#define TEST_START(name) \
    TEST_DEFINE(name) \
    { \
        InList<TestFunction>    tests; \
        TestFunction*           testit; \
        int                     ret;

#define TEST_LOG_INIT(targets) \
    Log_SetTimestamping(true); \
    Log_SetTarget(targets); \
    //Log_SetTrace(true); 


#define TEST_EXECUTE() \
    ret = TEST_SUCCESS; \
    StartClock(); \
    FOREACH(testit, tests) \
    { \
        ret = test_time(testit->function, testit->name); \
        if (ret != TEST_SUCCESS) \
            break; \
    } \
    StopClock(); \
    return test_eval(TEST_NAME, ret); \
}

#define TEST_ADD(testfn) \
    extern int testfn(); \
    TestFunction TEST_CONCAT(testfn, Function)(testfn, TEST_MAKENAME(testfn)); \
    tests.Append(&TEST_CONCAT(testfn, Function));
    
#endif
