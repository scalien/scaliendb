#ifndef LOG_H
#define LOG_H

/*
===============================================================================================

 Log: logging subsystem, uses Formatting.h

===============================================================================================
*/

#define LOG_TYPE_ERRNO      0
#define LOG_TYPE_MSG        1
#define LOG_TYPE_TRACE      2
#define LOG_TYPE_DEBUG      3

#define LOG_TARGET_NOWHERE  0
#define LOG_TARGET_STDOUT   1
#define LOG_TARGET_STDERR   2
#define LOG_TARGET_FILE     4
#define LOG_TARGET_SYSLOG   8

#ifdef NO_LOGGING // start NO_LOGGING

#define Log_Errno(...)
#define Log_Message(...)
#define Log_Trace(...)
#define Log_Debug(...)

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

#define Log_Debug(...) \
    Log(__FILE__, __LINE__, __func__, LOG_TYPE_DEBUG, "" __VA_ARGS__)

#endif // end LOGGING

#ifdef __cplusplus
extern "C" {
#endif

void Log(const char* file, int line, const char* func, int type, const char* fmt, ...);
bool Log_SetTrace(bool trace);
bool Log_SetDebug(bool debug);
bool Log_SetAutoFlush(bool autoFlush);
void Log_SetTimestamping(bool ts);
void Log_SetThreadedOutput(bool to);
void Log_SetMaxLine(int maxLine);
void Log_SetTraceBufferSize(unsigned traceBufferSize);
void Log_SetTarget(int target);
int  Log_GetTarget();
bool Log_SetOutputFile(const char* file, bool truncate);
void Log_SetMaxSize(unsigned maxSizeMB);    // in MegaBytes
void Log_Flush();
void Log_Shutdown();

#ifdef __cplusplus
}
#endif


#endif
