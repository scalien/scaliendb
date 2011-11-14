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
//#define strdup _strdup
#define strerror_r(errno, buf, buflen) strerror_s(buf, buflen, errno)
typedef unsigned __int64    uint64_t;
typedef __int64             int64_t;
#define Log_GetThreadID() (uint64_t)(GetCurrentThreadId())
#else
#include <sys/time.h>
#include <sys/stat.h>
#include <pthread.h>
#include <stdint.h>
#define Log_GetThreadID() ((uint64_t)(pthread_self()))
#endif

#include "Log.h"
#include "Formatting.h"
#include "System/Threading/Mutex.h"

#define LOG_MSG_SIZE    1024
#define LOG_OLD_EXT     ".old"

static bool     timestamping = false;
static bool     threadedOutput = false;
static bool     trace = false;
#ifdef DEBUG
static bool     debug = true;
#else
static bool     debug = false;
#endif
static int      maxLine = LOG_MSG_SIZE;
static int      target = LOG_TARGET_NOWHERE;
static FILE*    logfile = NULL;
static char*    logfilename = NULL;
static uint64_t maxSize = 0;
static uint64_t logFileSize = 0;
static Mutex    logFileMutex;
static bool     autoFlush = true;

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

// These functions are duplicates of the functions in FileSystem, but here they don't use logging obviously
#ifdef _WIN32
static bool Log_RenameFile(const char* src, const char* dst)
{
    BOOL    ret;
    
    ret = MoveFileEx(src, dst, MOVEFILE_WRITE_THROUGH);
    if (!ret)
        return false;
    
    return true;
}

static bool Log_DeleteFile(const char* filename)
{
    BOOL    ret;
    
    ret = DeleteFile(filename);
    if (!ret)
        return false;
    
    return true;
}

static int64_t Log_FileSize(const char* path)
{
    WIN32_FILE_ATTRIBUTE_DATA   attrData;
    BOOL                        ret;

    ret = GetFileAttributesEx(path, GetFileExInfoStandard, &attrData);
    if (!ret)
        return -1;
    
    return ((int64_t) attrData.nFileSizeHigh) << 32 | attrData.nFileSizeLow;
}

#else

static bool Log_RenameFile(const char* src, const char* dst)
{
    int     ret;
    
    ret = rename(src, dst);
    if (ret < 0)
        return false;
    
    return true;
}

static bool Log_DeleteFile(const char* filename)
{
    int ret;
    
    ret = unlink(filename);
    if (ret < 0)
        return false;
    
    return true;
}

static int64_t Log_FileSize(const char* path)
{
    int64_t     ret;
    struct stat buf;
    
    ret = stat(path, &buf);
    if (ret < 0)
        return ret;
    
    return buf.st_size;
}

#endif


static void Log_Append(char*& p, int& remaining, const char* s, int len)
{
    if (len > remaining)
        len = remaining;
        
    if (len > 0)
        memcpy(p, s, len);
    
    p += len;
    remaining -= len;
}

static void Log_Rotate()
{
    char*   filenameCopy;
    char*   oldFilename;
    size_t  oldFilenameSize;
    char*   oldOldFilename;
    size_t  oldOldFilenameSize;
    size_t  filenameLen;
    size_t  extLen;

    fprintf(stderr, "Rotating...\n");

    filenameCopy = strdup(logfilename);

    filenameLen = strlen(logfilename);
    extLen = sizeof(LOG_OLD_EXT) - 1;
    
    oldFilenameSize = filenameLen + extLen + 1;
    oldFilename = new char[oldFilenameSize]; 
    snprintf(oldFilename, oldFilenameSize, "%s" LOG_OLD_EXT, logfilename); 
    
    oldOldFilenameSize = filenameLen + extLen + extLen + 1;
    oldOldFilename = new char[oldOldFilenameSize];
    snprintf(oldOldFilename, oldOldFilenameSize, "%s" LOG_OLD_EXT LOG_OLD_EXT, logfilename);

    // delete any previously created temporary file
    Log_DeleteFile(oldOldFilename);

    // rename the old version to a temporary name
    if (!Log_RenameFile(oldFilename, oldOldFilename))
    {
        Log_DeleteFile(oldFilename);
    }

    // close the current file
    if (logfile)
    {
        fclose(logfile);
        logfile = NULL;
    }

    // rename the current to old
    if (!Log_RenameFile(logfilename, oldFilename))
    {
        // TODO:
    }

    // create a new file
    Log_SetOutputFile(filenameCopy, true);

    // delete any previously created temporary file
    Log_DeleteFile(oldOldFilename);

    // cleanup
    free(filenameCopy);
    delete[] oldFilename;
    delete[] oldOldFilename;
}

static void Log_Write(const char* buf, int size, int flush)
{
    if ((target & LOG_TARGET_STDOUT) == LOG_TARGET_STDOUT)
    {
        if (buf)
            fputs(buf, stdout);
        if (flush)
			fflush(stdout);
    }
    
    if ((target & LOG_TARGET_STDERR) == LOG_TARGET_STDERR)
    {
        if (buf)
            fputs(buf, stderr);
		if (flush)
			fflush(stderr);
    }
    
    if ((target & LOG_TARGET_FILE) == LOG_TARGET_FILE && logfile)
    {
        logFileMutex.Lock();

        if (buf)
            fputs(buf, logfile);
		if (flush)
			fflush(logfile);
        
        if (maxSize > 0)
        {
            
            // we keep the previous logfile, hence the division by two
            if (logFileSize + size > maxSize / 2)
            {
                // rotate the logfile
                Log_Rotate();
            }
            else
            {
                logFileSize += size;
            }
        }

        logFileMutex.Unlock();
    }
}

void Log_SetTimestamping(bool ts)
{
    timestamping = ts;
}

void Log_SetThreadedOutput(bool to)
{
    threadedOutput = to;
}

bool Log_SetTrace(bool trace_)
{
    bool prev = trace;
    
    trace = trace_;
    return prev;
}

bool Log_SetDebug(bool debug_)
{
    bool prev = debug;

    debug = debug_;
    return prev;
}

bool Log_SetAutoFlush(bool autoFlush_)
{
    bool prev = autoFlush;

    autoFlush = autoFlush_;
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

int Log_GetTarget()
{
    return target;
}

bool Log_SetOutputFile(const char* filename, bool truncate)
{
    if (logfile)
    {
        fclose(logfile);
        free(logfilename);
        logfilename = NULL;
    }

    if (!filename)
        return false;

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
    logFileSize = Log_FileSize(filename);
    return true;
}

void Log_SetMaxSize(unsigned maxSizeMB)
{
    maxSize = ((uint64_t)maxSizeMB) * 1000 * 1000;
}

void Log_Flush()
{
    Log_Write(NULL, 0, true);
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

void Log(const char* file, int line, const char* func, int type, const char* fmt, ...)
{
    char        buf[LOG_MSG_SIZE];
    int         remaining;
    char        *p;
    const char  *sep;
    int         ret;
    va_list     ap;
    uint64_t    threadID;

    // In debug mode enable ERRNO type messages
    if ((type == LOG_TYPE_TRACE || type == LOG_TYPE_ERRNO) && !trace)
        return;

    if ((type == LOG_TYPE_DEBUG) && !debug)
        return;

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

    // print threadID
    if (threadedOutput)
    {
        threadID = Log_GetThreadID();
        ret = Writef(p, remaining, "[%U]: ", threadID);
        if (ret < 0 || ret > remaining)
            ret = remaining;

        p += ret;
        remaining -= ret;
    }

    // don't print filename and func in debug mode on ERRNO messages
    if (file && func && (type == LOG_TYPE_TRACE || (type == LOG_TYPE_ERRNO && trace)))
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
        ret = snprintf(p, remaining, "Error %u: ", lastError);
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
    if (autoFlush)
        Log_Write(buf, maxLine - remaining, type != LOG_TYPE_TRACE && type != LOG_TYPE_DEBUG);
    else
        Log_Write(buf, maxLine - remaining, false);
}
