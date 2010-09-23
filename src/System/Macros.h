#ifndef MACROS_H
#define MACROS_H

/*
 ==============================================================================
 */

#define MEMCMP(b1, l1, b2, l2)      ((l1) == (l2) && memcmp((b1), (b2), l1) == 0)

/*
 ==============================================================================
 */

#define BUFCMP(b1, b2)  \
 ((b1)->GetLength() == (b2)->GetLength() && \
 memcmp((b1)->GetBuffer(), (b2)->GetBuffer(), (b1)->GetLength()) == 0)

/*
 ==============================================================================
 */

#define SIZE(a)                     (sizeof((a)) / sizeof((a[0])))

/*
 ==============================================================================
 */

#define ASSERT_FAIL()               assert(false)

/*
 ==============================================================================
 */

#define STOP_FAIL(code, ...) \
{ \
    Log_SetTarget(LOG_TARGET_STDERR|LOG_TARGET_FILE); \
    Log_Message(__VA_ARGS__); \
    _exit(code); \
}

/*
 ==============================================================================
 */

#define RESTART(msg) \
{ \
    Log_Message(msg); \
    _exit(2); \
}

/*
 ==============================================================================
 */

#define CS_INT_SIZE(int_type) \
 ((size_t)(0.30103 * sizeof(int_type) * 8) + 2 + 1)

/*
 ==============================================================================
 */

#define ABS(a)      ((a) > 0 ? (a) : (-1*(a)))
#define MIN(a, b)   ((a) < (b) ? (a) : (b))
#define MAX(a, b)   ((a) > (b) ? (a) : (b))

/*
 ==============================================================================
 */
 
#define STR_AND_LEN(s) s, strlen(s)

/*
 ==============================================================================

  It is  a common idiom to use const char string literals (CSL) in the code, 
  and the length of const literals are known at compile time.
 
 ============================================================================== 
 */

#define CSLLEN(s) (sizeof(s "") - 1)
#define STR_AND_CSLLEN(s) s, CSLLEN(s)

#endif
