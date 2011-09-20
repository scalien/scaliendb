#include "Log.h"
#include "Formatting.h"
#define _XOPEN_SOURCE 600
#include <string.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <time.h>
#ifdef _WIN32
#include <windows.h>
#define snprintf _snprintf
#define strdup _strdup
#define strerror_r(errno, buf, buflen) strerror_s(buf, buflen, errno)
#else
#include <sys/time.h>
#endif

#define LOG_MSG_SIZE    1024

static bool     timestamping = false;
static bool     trace = false;
static int      maxLine = LOG_MSG_SIZE;
static int      target = LOG_TARGET_NOWHERE;
static FILE*    logfile = NULL;
static char*    logfilename = NULL;

#ifdef _WIN32
typedef char log_timestamp_t[24];
#else
typedef char log_timestamp_t[27];
#endif

static const char* GetFullTimestamp(log_timestamp_t ts)
{
    if (!timestamping)
        return "";

#ifdef _WIN32
    SYSTEMTIME  st;
    GetLocalTime(&st);
    
    snprintf(ts, sizeof(log_timestamp_t), "%04d-%02d-%02d %02d:%02d:%02d.%03d",
     (int) st.wYear,
     (int) st.wMonth,
     (int) st.wDay,
     (int) st.wHour,
     (int) st.wMinute,
     (int) st.wSecond,
     (int) st.wMilliseconds);
#else
    struct tm tm;
    time_t sec;
    struct timeval tv;

    gettimeofday(&tv, NULL);
    sec = (time_t) tv.tv_sec;
    localtime_r(&sec, &tm);

    snprintf(ts, sizeof(log_timestamp_t), "%04d-%02d-%02d %02d:%02d:%02d.%06lu", 
     tm.tm_year + 1900,
     tm.tm_mon + 1,
     tm.tm_mday,
     tm.tm_hour,
     tm.tm_min,
     tm.tm_sec,
     (long unsigned int) tv.tv_usec);
#endif
    
    return ts;
}

void Log_SetTimestamping(bool ts)
{
    timestamping = ts;
}

bool Log_SetTrace(bool trace_)
{
    bool prev = trace;
    
    trace = trace_;
    return prev;
}

void Log_SetMaxLine(int maxLine_)
{
    maxLine = maxLine_ > LOG_MSG_SIZE ? LOG_MSG_SIZE : maxLine_;
}

void Log_SetTarget(int target_)
{
    target = target_;
}

bool Log_SetOutputFile(const char* filename, bool truncate)
{
    if (logfile)
    {
        fclose(logfile);
        free(logfilename);
        logfilename = NULL;
    }

    if (truncate)
        logfile = fopen(filename, "w");
    else
        logfile = fopen(filename, "a");
    if (!logfile)
    {
        target &= ~LOG_TARGET_FILE;
        return false;
    }
    
    logfilename = strdup(filename);
    return true;
}

void Log_Shutdown()
{
    if (logfilename)
    {
        free(logfilename);
        logfilename = NULL;
    }
    
    if (logfile)
    {
        fclose(logfile);
        logfile = NULL;
    }
    
    fflush(stdout);
    fflush(stderr);
    
    trace = false;
    timestamping = false;
}

static void Log_Append(char*& p, int& remaining, const char* s, int len)
{
    if (len > remaining)
        len = remaining;
        
    if (len > 0)
        memcpy(p, s, len);
    
    p += len;
    remaining -= len;
}

static void Log_Write(const char* buf, int /*size*/, int flush)
{
    if ((target & LOG_TARGET_STDOUT) == LOG_TARGET_STDOUT)
    {
        fputs(buf, stdout);
        if (flush)
			fflush(stdout);
    }
    
    if ((target & LOG_TARGET_STDERR) == LOG_TARGET_STDERR)
    {
        fputs(buf, stderr);
		if (flush)
			fflush(stderr);
    }
    
    if ((target & LOG_TARGET_FILE) == LOG_TARGET_FILE && logfile)
    {
        fputs(buf, logfile);
		if (flush)
			fflush(logfile);
    }
}

void Log(const char* file, int line, const char* func, int type, const char* fmt, ...)
{
    char        buf[LOG_MSG_SIZE];
    int         remaining;
    char        *p;
    const char  *sep;
    int         ret;
    va_list     ap;

    // In debug mode enable ERRNO type messages
#ifdef DEBUG
    if (type == LOG_TYPE_TRACE && !trace)
        return;
#else
    if ((type == LOG_TYPE_TRACE || type == LOG_TYPE_ERRNO) && !trace)
        return;
#endif

    buf[maxLine - 1] = 0;
    p = buf;
    remaining = maxLine - 1;

    // print timestamp
    if (timestamping)
    {
        GetFullTimestamp(p);
        p += sizeof(log_timestamp_t) - 1;
        remaining -= sizeof(log_timestamp_t) - 1;
        Log_Append(p, remaining, ": ", 2);
    }

    if (type != LOG_TYPE_MSG && file && func)
    {
#ifdef _WIN32
        sep = strrchr(file, '/');
        if (!sep)
            sep = strrchr(file, '\\');
#else
        sep = strrchr(file, '/');
#endif
        if (sep)
            file = sep + 1;
        
        // print filename, number of line and function name
        ret = snprintf(p, remaining + 1, "%s:%d:%s()", file, line, func);
        if (ret < 0 || ret > remaining)
            ret = remaining;

        p += ret;
        remaining -= ret;

        if (fmt[0] != '\0' || type == LOG_TYPE_ERRNO)
            Log_Append(p, remaining, ": ", 2);
    }
    
    // in case of error print the errno message otherwise print our message
    if (type == LOG_TYPE_ERRNO)
    {
#ifdef _GNU_SOURCE
        // this is a workaround for g++ on Debian Lenny
        // see http://bugs.debian.org/cgi-bin/bugreport.cgi?bug=485135
        // _GNU_SOURCE is unconditionally defined so always the GNU style
        // sterror_r() is used, which is broken
        char* err = strerror_r(errno, p, remaining - 1);
        if (err)
        {
                ret = strlen(err);
                if (err != p)
                {
                        memcpy(p, err, ret);
                        p[ret] = 0;
                }
        }
        else
                ret = -1;
#elif _WIN32
        DWORD lastError = GetLastError();
        ret = snprintf(p, remaining, "%u: ", lastError);
        if (ret < 0 || ret >= remaining)
            ret = remaining - 1;

        p += ret;
        remaining -= ret;
        if (remaining > 2)
        {
            ret = FormatMessage(FORMAT_MESSAGE_FROM_SYSTEM |
                FORMAT_MESSAGE_IGNORE_INSERTS |
                (FORMAT_MESSAGE_MAX_WIDTH_MASK & remaining - 2),
                NULL,
                lastError,
                MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
                (LPTSTR) p,
                remaining - 2,
                NULL);
        }
        else
            ret = 0;
#else
        ret = strerror_r(errno, p, remaining - 1);
        if (ret >= 0)
            ret = (int) strlen(p);
#endif
        if (ret < 0)
            ret = remaining;

        p += ret;
        remaining -= ret;
        if (fmt[0] != '\0')
            Log_Append(p, remaining, ": ", 2);
    }
//    else
    {
        va_start(ap, fmt);
        ret = VWritef(p, remaining, fmt, ap);
        va_end(ap);
        if (ret < 0 || ret >= remaining)
            ret = remaining - 1;

        p += ret;
        remaining -= ret;
    }

    Log_Append(p, remaining, "\n", 2);
    Log_Write(buf, maxLine - remaining, type != LOG_TYPE_TRACE);
}
