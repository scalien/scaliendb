#ifndef LOG_H
#define LOG_H

/*
===============================================================================================

 Log: our own logging subsystem, uses Formatting.h

===============================================================================================
*/

#define LOG_TYPE_ERRNO      0
#define LOG_TYPE_MSG        1
#define LOG_TYPE_TRACE      2

#define LOG_TARGET_NOWHERE  0
#define LOG_TARGET_STDOUT   1
#define LOG_TARGET_STDERR   2
#define LOG_TARGET_FILE     4
#define LOG_TARGET_SYSLOG   8
#define LOG_TARGET_FUNCTION 16

#ifdef NO_LOGGING // start NO_LOGGING

#define Log_Errno()
#define Log_Message(...)
#define Log_Trace(...)

#else // end NO_LOGGING, start LOGGING

#ifdef _WIN32
#define __func__ __FUNCTION__
#endif

#define Log_Errno(...) \
    Log(__FILE__, __LINE__, __func__, LOG_TYPE_ERRNO, "" __VA_ARGS__)

#define Log_Message(...) \
    Log(__FILE__, __LINE__, __func__, LOG_TYPE_MSG, "" __VA_ARGS__)

#define Log_Trace(...) \
    Log_Trace_("" __VA_ARGS__)

#define Log_Trace_(...) \
    Log(__FILE__, __LINE__, __func__, LOG_TYPE_TRACE, __VA_ARGS__)

#ifdef DEBUG
#define Log_Debug Log_Message
#else
#define Log_Debug Log_Trace
#endif

#endif // end LOGGING

#ifdef GCC
#define ATTRIBUTE_FORMAT_PRINTF(fmt, ellipsis) \
    __attribute__ ((format (printf, fmt, ellipsis)));
#else
#define ATTRIBUTE_FORMAT_PRINTF(fmt, ellipsis)
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef void (*LoggerFunction)(void* data, const char* buf, int size, int flush);

void Log(const char* file, int line, const char* func,
 int type, const char* fmt, ...);
bool Log_SetTrace(bool trace);
void Log_SetTimestamping(bool ts);
void Log_SetMaxLine(int maxLine);
void Log_SetTarget(int target);
bool Log_SetOutputFile(const char* file, bool truncate);
void Log_SetFunction(LoggerFunction func, void* data);
void Log_Shutdown();

#ifdef __cplusplus
}
#endif


#endif
