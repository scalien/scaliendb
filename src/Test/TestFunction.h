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

inline int TestMatchName(int argc, char* argv[], TestFunction& testFunction)
{

    for (int i = 1; i < argc; i++)
    {
        char* prefix = NULL;
        size_t len = strlen(argv[i]);
        // prefix matching
        if (len > 0 && argv[i][len - 1] == '*')
        {
            prefix = strdup(argv[i]);
            prefix[len - 1] = '\0';
            if (strncmp(prefix, testFunction.name, len - 1) == 0)
            {                
                free(prefix);
                return 1;
            }
            free(prefix);
        }
        else if (strcmp(argv[i], testFunction.name) == 0)
        {
            return 1;
        }
    }
    return 0;
}

#define TEST_START(name)                    \
    extern "C" int name(int argc, char* argv[])        \
    {                                       \
        InList<TestFunction>    tests;      \
        TestFunction*           testit;     \
        unsigned                numSuccess; \
        int                     ret;

#define TEST_LOG_INIT(trace, targets)                   \
    if ((targets & LOG_TARGET_FILE) == LOG_TARGET_FILE) \
        Log_SetOutputFile("__Test.log", true);          \
    Log_SetTimestamping(true);                          \
    Log_SetTarget(targets);                             \
    Log_SetTrace(trace); 


#define TEST_EXECUTE()                                      \
    ret = TEST_SUCCESS;                                     \
    StartClock();                                           \
    numSuccess = 0;                                         \
    FOREACH (testit, tests)                                 \
    {                                                       \
        ret = test_time(testit->function, testit->name);    \
        if (ret != TEST_SUCCESS)                            \
            break;                                          \
        numSuccess++;                                       \
    }                                                       \
    StopClock();                                            \
    printf("test: Summary: %u/%u test%s succeeded\n", numSuccess, tests.GetLength(), numSuccess > 1 ? "s" : ""); \
    return test_eval(TEST_NAME, ret);                       \
}

#define TEST_ADD(testfn)                                                        \
    extern int testfn();                                                        \
    TestFunction TEST_CONCAT(testfn, Function)(testfn, TEST_MAKENAME(testfn));  \
    for (int i = 1; i < argc; i++)                                              \
    {                                                                           \
        if (TestMatchName(argc, argv, TEST_CONCAT(testfn, Function)))           \
        {                                                                       \
            tests.Append(&TEST_CONCAT(testfn, Function));                       \
            break;                                                              \
        }                                                                       \
    }
    
#endif
