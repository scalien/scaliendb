#include "Common.h"
#ifndef _WIN32
#include <pwd.h>
#include <signal.h>
#include <execinfo.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/sysctl.h>
#include <sys/resource.h>
#else // _WIN32
#include <process.h>
#include <windows.h>
#include <psapi.h>
#include <dbghelp.h>
#endif
#ifdef PLATFORM_DARWIN
#include <mach/mach.h>
#endif

#include <stdio.h>
#include <string>
#include <stdlib.h>
#include <math.h>

#include "Macros.h"
#include "Time.h"
#include "Buffers/Buffer.h"
#include "Buffers/ReadBuffer.h"

static bool exitOnError = false;
static bool isAssertCritical = true;

void Error()
{
    if (exitOnError)
        _exit(1);   // exit immediately
}

void SetExitOnError(bool exitOnError_)
{
    exitOnError = exitOnError_;
}

void SetAssertCritical(bool isAssertCritical_)
{
    isAssertCritical = isAssertCritical_;
}

bool IsAssertCritical()
{
    return isAssertCritical;
}

unsigned NumDigits(int n)
{
    return n == 0 ? 1 : (unsigned) floor(log10((float)n) + 1);
}

unsigned NumDigits64(uint64_t n)
{
    unsigned    d;
    
    if (n == 0)
        return 1;

    d = 0;
    while (n > 0)
    {
        n = n / 10;
        d++;
    }
    return d;
}

const char* HumanBytes(uint64_t bytes, char buf[5])
{
    const char  units[] = "kMGTPEZY";
    uint64_t    n;
    float       f;
    unsigned    u;
    unsigned    ndigits;
    
    n = bytes;
    f = bytes;
    u = 0;
    while ((ndigits = NumDigits64(n)) > 3)
    {
        n = (uint64_t)(n / 1000.0 + 0.5);   // rounding
        f = f / 1000.0;
        u++;
    }

    if (ndigits == 1 && bytes >= 10)
        snprintf(buf, 5, "%1.1f%c", f, u == 0 ? units[sizeof(units) - 1] : units[u - 1]);
    else
        snprintf(buf, 5, "%" PRIu64 "%c", n, u == 0 ? units[sizeof(units) - 1] : units[u - 1]);
    return buf;
}

const char* SIBytes(uint64_t bytes, char buf[5])
{
    const char  units[] = "kMGTPEZY";
    uint64_t    n;
    unsigned    u;
    
    n = bytes;
    u = 0;
    while (NumDigits64(n) > 3)
    {
        n = (uint64_t)(n / 1024.0 + 0.5);   // rounding
        u++;
    }
    
    snprintf(buf, 5, "%" PRIu64 "%c", n, u == 0 ? units[sizeof(units) - 1] : units[u - 1]);
    return buf;
}

const char* HumanTime(char buf[27])
{
#ifdef _WIN32
    SYSTEMTIME  st;

    GetLocalTime(&st);
    
    snprintf(buf, 27, "%04d-%02d-%02d %02d:%02d:%02d.%03d",
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

    snprintf(buf, 27, "%04d-%02d-%02d %02d:%02d:%02d.%06lu", 
     tm.tm_year + 1900,
     tm.tm_mon + 1,
     tm.tm_mday,
     tm.tm_hour,
     tm.tm_min,
     tm.tm_sec,
     (long unsigned int) tv.tv_usec);
#endif
    
    return buf;
}

int64_t BufferToInt64(const char* buffer, unsigned length, unsigned* nread)
{
    bool        neg;
    long        digit;
    unsigned    i;
    int64_t     n;
    const char* c;

    if (buffer == NULL || length < 1)
    {
        *nread = 0;
        return 0;
    }
    
    n = 0;
    i = 0;
    c = buffer;
    
    if (*c == '-')
    {
        neg = true;
        i++;
        c = buffer + i;
    }
    else
        neg = false;
    
    while (i < length && *c >= '0' && *c <= '9')
    {
        digit = *c - '0';
        n = n * 10 + digit;
        i++;
        c = buffer + i;
    }
    
    if (neg && i == 1)
    {
        *nread = 0;
        return 0;
    }
    
    *nread = i;

    if (neg)
        return -n;
    else
        return n;
}

uint64_t BufferToUInt64(const char* buffer, unsigned length, unsigned* nread)
{
    long        digit;
    unsigned    i;
    uint64_t    n;
    const char* c;

    if (buffer == NULL || length < 1)
    {
        *nread = 0;
        return 0;
    }

    n = 0;
    i = 0;
    c = buffer;
    
    while (i < length && *c >= '0' && *c <= '9')
    {
        digit = *c - '0';
        n = n * 10 + digit;
        i++;
        c = buffer + i;
    }
    
    *nread = i;

    return n;
}

// this works the same as snprintf(buf, bufsize, "%" PRIu64, value) would do
int UInt64ToBufferWithBase(char* buf, unsigned bufsize, uint64_t value, char base)
{
    char        tmp[64 + 1];
    char        charset[] = "01234567890ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_-";
    unsigned    d;

    // special case
    if (value == 0)
    {
        if (bufsize == 0)
            return 1;
            
        if (bufsize > 0)
            buf[0] = '0';
        else
            buf[0] = 0;
            
        if (bufsize > 1)
            buf[1] = 0;

        return 1;
    }

    // write digits to reverse order in temp buffer
    d = 0;
    while (value > 0)
    {
        tmp[d] = charset[value % base];
        d += 1;
        value /= base;
    }
    
    // copy the digits
    for (unsigned i = 0; i < d; i++)
    {
        if (i < bufsize)
            buf[i] = tmp[d - 1 - i];
    }

    // terminate with zero
    if (d < bufsize)
        buf[d] = 0;
    else
        buf[bufsize - 1] = 0;

    return d;
}

char* FindInBuffer(const char* buffer, unsigned length, char c)
{
    size_t  i;
    
    for (i = 0; i < length; i++)
        if (buffer[i] == c) return (char*) (buffer + i);
        
    return NULL;
}

char* RevFindInBuffer(const char* buffer, unsigned length, char c)
{
    size_t  i;
    
    if (length == 0)
        return NULL;

    for (i = length; i > 0; i--)
        if (buffer[i - 1] == c) return (char*) (buffer + i - 1);
        
    return NULL;
}

char* FindInCString(const char* s, char c)
{
    return (char*) strchr(s, c);
}

void ReplaceInBuffer(char* buffer, unsigned length, char src, char dst)
{
    unsigned i;

    for (i = 0; i < length; i++)
        if (buffer[i] == src)
            buffer[i] = dst;
}

void ReplaceInCString(char* s, char src, char dst)
{
    ReplaceInBuffer(s, strlen(s), src, dst);
}

bool RangeContains(ReadBuffer firstKey, ReadBuffer lastKey, ReadBuffer key)
{
    int         cf, cl;
    
#define NOT_SET     -9999
#define COMP_CF()   { if (cf == NOT_SET) cf = ReadBuffer::Cmp(firstKey, key); }
#define COMP_CL()   { if (cl == NOT_SET) cl = ReadBuffer::Cmp(key, lastKey);  }

    cf = NOT_SET;
    cl = NOT_SET;
    
    if (firstKey.GetLength() == 0)
    {
        if (lastKey.GetLength() == 0)
        {
            return true;
        }
        else
        {
            COMP_CL();
            return (cl < 0);        // (key < lastKey);
        }
    }
    else if (lastKey.GetLength() == 0)
    {
        COMP_CF();
        return (cf <= 0);           // (firstKey <= key);
    }
    else
    {
        COMP_CF();
        if (cf > 0)
            return false;
        COMP_CL();
        return (cl < 0);
    }

#undef NOT_SET
#undef COMP_CF
#undef COMP_CL
}

const char* StaticPrint(const char* format, ...)
{
    static char buffer[8*1024];
    va_list     ap;
    
    va_start(ap, format);
    VWritef(buffer, sizeof(buffer), format, ap);
    va_end(ap);
    
    return buffer;
}

const char* InlinePrintf(char* buffer, size_t size, const char* format, ...)
{
    va_list     ap;

    va_start(ap, format);
    vsnprintf(buffer, size, format, ap);
    va_end(ap);
    
    return buffer;
}

uint64_t GenerateGUID()
{
    const uint64_t WIDTH_M = 16; // machine TODO
    const uint64_t MASK_M = ((uint64_t) 1 << WIDTH_M) - 1;
    const uint64_t WIDTH_D = 32; // date
    const uint64_t MASK_D = ((uint64_t) 1 << WIDTH_D) - 1;
    const uint64_t WIDTH_R = 16; // random
    const uint64_t MASK_R = ((uint64_t) 1 << WIDTH_R) - 1;
    uint64_t uuid;
    
    uint64_t m = RandomInt(0, 65535); // TODO
    uint64_t d = Now();
    uint64_t r = RandomInt(0, 65535);

    uuid = 0;
    uuid |= (m & MASK_M) << (WIDTH_D + WIDTH_R);
    uuid |= (d & MASK_D) << (WIDTH_R);
    uuid |= (r & MASK_R);

    return uuid;
}

#ifdef _WIN32
#define srandom srand
#endif

void SeedRandom()
{
    unsigned seed = (unsigned)(Now() & 0xFFFFFFFF);
    srandom(seed);
}

void SeedRandomWith(uint64_t seed)
{
    srandom((unsigned) seed);
}

int RandomInt(int min, int max)
{
    int             rnd;
    int             interval;
    int             rndout;
    const double    DIV = (double)RAND_MAX + 1;
    double          tmp;
    
    ASSERT(min < max);

    interval = (max - min) + 1;
    
#ifndef _WIN32
    rnd = random(); 
#else
    rnd = rand();
#endif
    tmp = rnd / DIV;
    rndout = (int)floor(tmp * interval);
    return rndout + min;
}

void RandomBuffer(char* buffer, unsigned length)
{
    const char set[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    const size_t setsize = sizeof(set) - 1;
    unsigned int i;
    static uint64_t d = GenerateGUID();
    
    for (i = 0; i < length; i++) {
            // more about why these numbers were chosen:
            // http://en.wikipedia.org/wiki/Linear_congruential_generator
            d = (d * 1103515245UL + 12345UL) >> 2;
            buffer[i] = set[d % setsize];
    }
}

void BlockSignals()
{
#ifdef _WIN32
    // dummy
#else
    sigset_t    sigmask;
    
    // block all signals
    sigfillset(&sigmask);
    pthread_sigmask(SIG_SETMASK, &sigmask, NULL);
#endif
}

bool ChangeUser(const char *user)
{
#ifdef _WIN32
    (void) user;
    // cannot change user on Windows
    return true;
#else
    if (user != NULL && *user && (getuid() == 0 || geteuid() == 0)) 
    {
        struct passwd *pw = getpwnam(user);
        if (!pw)
            return false;
        
        setuid(pw->pw_uid);
    }

    return true;
#endif
}

const char* GetStackTrace(char* buffer, int size, const char* prefix)
{
#ifdef _WIN32
    unsigned int        i;
    void*               stack[100];
    unsigned short      frames;
    char                symbolBuffer[sizeof(SYMBOL_INFO) + MAX_SYM_NAME * sizeof(TCHAR)];
    SYMBOL_INFO*        symbol;
    HANDLE              process;
    IMAGEHLP_LINE64     line;
    IMAGEHLP_MODULE64   module;
    DWORD               displacement;
    const char*         fileName;
    DWORD64             address;
    int                 ret;
    char*               p;

    if (prefix == NULL)
        prefix = "";

    process = GetCurrentProcess();
    SymInitialize(process, NULL, TRUE);
    SymSetOptions(SYMOPT_LOAD_LINES);

    // http://msdn.microsoft.com/en-us/windows/bb204633
    // Windows Server 2003 and Windows XP:  The sum of the FramesToSkip and FramesToCapture 
    // parameters must be less than 63.
    frames = CaptureStackBackTrace(0, 62, stack, NULL);
    symbol = (SYMBOL_INFO*) symbolBuffer;
    symbol->MaxNameLen = MAX_SYM_NAME;
    symbol->SizeOfStruct = sizeof(SYMBOL_INFO);

    line.SizeOfStruct = sizeof(IMAGEHLP_LINE64);
    module.SizeOfStruct = sizeof(IMAGEHLP_MODULE64);

    p = buffer;
    // skip current function, therefore start from 1
    for (i = 2; i < frames; i++)
    {
        address = (DWORD64)(stack[i]);

        SymFromAddr(process, address, 0, symbol);
        SymGetLineFromAddr64(process, address, &displacement, &line);
        SymGetModuleInfo64(process, address, &module);

        fileName = strrchr(module.ImageName, '\\');
        fileName = fileName ? fileName + 1 : module.ImageName;
        
        ret = snprintf(p, size, "%s%s!%s()  Line %u\n", prefix, fileName, symbol->Name, line.LineNumber);
        if (ret < 0 || ret == 0)
            return "";  // error

        if (ret >= size)
            return buffer;  // truncation

        p += ret;
        size -= ret;
    }

    return buffer;
#else
    // TODO: 
    return "";
#endif
}

void PrintStackTrace()
{
#ifdef _WIN32
    unsigned int        i;
    void*               stack[100];
    unsigned short      frames;
    char                symbolBuffer[sizeof(SYMBOL_INFO) + MAX_SYM_NAME * sizeof(TCHAR)];
    SYMBOL_INFO*        symbol;
    HANDLE              process;
    IMAGEHLP_LINE64     line;
    IMAGEHLP_MODULE64   module;
    DWORD               displacement;
    const char*         fileName;
    DWORD64             address;

    process = GetCurrentProcess();
    SymInitialize(process, NULL, TRUE);
    SymSetOptions(SYMOPT_LOAD_LINES);

    // http://msdn.microsoft.com/en-us/windows/bb204633
    // Windows Server 2003 and Windows XP:  The sum of the FramesToSkip and FramesToCapture 
    // parameters must be less than 63.
    frames = CaptureStackBackTrace(0, 62, stack, NULL);
    symbol = (SYMBOL_INFO*) symbolBuffer;
    symbol->MaxNameLen = MAX_SYM_NAME;
    symbol->SizeOfStruct = sizeof(SYMBOL_INFO);

    line.SizeOfStruct = sizeof(IMAGEHLP_LINE64);
    module.SizeOfStruct = sizeof(IMAGEHLP_MODULE64);

    // skip current function, therefore start from 1
    for (i = 2; i < frames; i++)
    {
        address = (DWORD64)(stack[i]);

        SymFromAddr(process, address, 0, symbol);
        SymGetLineFromAddr64(process, address, &displacement, &line);
        SymGetModuleInfo64(process, address, &module);

        fileName = strrchr(module.ImageName, '\\');
        fileName = fileName ? fileName + 1 : module.ImageName;
        
        fprintf(stderr, "%s!%s()  Line %u\n", fileName, symbol->Name, line.LineNumber);
    }      
#else
    void*   array[100];
    size_t  size;
    
    size = backtrace(array, SIZE(array));
    backtrace_symbols_fd(&array[1], size - 1, STDERR_FILENO);
#endif
}

int ShellExec(const char* cmdline)
{
#ifdef _WIN32
    Buffer  cmd;
    
    return (_spawnlp(_P_WAIT, "cmd", "/c", cmdline, NULL));
#else
    return system(cmdline);
#endif
}

uint64_t GetProcessID()
{
#ifdef _WIN32
    return (uint64_t) _getpid();
#else
    return (uint64_t) getpid();
#endif
}

uint64_t GetTotalPhysicalMemory()
{
#ifdef _WIN32
    MEMORYSTATUSEX  statex;

    statex.dwLength = sizeof(statex);
    GlobalMemoryStatusEx(&statex);
    return (uint64_t) statex.ullTotalPhys;
#else
#ifdef PLATFORM_LINUX
    uint64_t    pages;
    uint64_t    pageSize;
    
    pages = sysconf(_SC_PHYS_PAGES);
    pageSize = sysconf(_SC_PAGE_SIZE);
    return pages * pageSize;
#elif PLATFORM_DARWIN
    int         mib[2];
    uint64_t    mem;
    size_t      len;
    int         ret;
    
    mib[0] = CTL_HW;
    mib[1] = HW_MEMSIZE;
    len = sizeof(mem);
    ret = sysctl(mib, SIZE(mib), &mem, &len, NULL, 0);
    if (ret < 0)
        mem = 0;

    return mem;
#endif
#endif
}

uint64_t GetProcessMemoryUsage()
{
#ifdef _WIN32
    PROCESS_MEMORY_COUNTERS     memCounters;

    GetProcessMemoryInfo(GetCurrentProcess(), &memCounters, sizeof(memCounters));
    return (uint64_t) memCounters.WorkingSetSize;
#else
#ifdef PLATFORM_DARWIN
    task_t targetTask = mach_task_self();
    struct task_basic_info ti;
    mach_msg_type_number_t count = TASK_BASIC_INFO_64_COUNT;

    task_info(targetTask, TASK_BASIC_INFO_64, (task_info_t) &ti, &count);
    
    return ti.resident_size;
#else
    return 0;
#endif
#endif
}

void SetMemoryLimit(uint64_t limit)
{
#ifdef _WIN32
    // Windows does not have this feature
#else
    struct rlimit   rlim;
    int             ret;
    
    rlim.rlim_cur = limit;
    ret = setrlimit(RLIMIT_AS, &rlim);
    if (ret < 0)
        Log_Errno();
#endif
}

static uint32_t const crctab[256] =
{
  0x00000000,
  0x04c11db7, 0x09823b6e, 0x0d4326d9, 0x130476dc, 0x17c56b6b,
  0x1a864db2, 0x1e475005, 0x2608edb8, 0x22c9f00f, 0x2f8ad6d6,
  0x2b4bcb61, 0x350c9b64, 0x31cd86d3, 0x3c8ea00a, 0x384fbdbd,
  0x4c11db70, 0x48d0c6c7, 0x4593e01e, 0x4152fda9, 0x5f15adac,
  0x5bd4b01b, 0x569796c2, 0x52568b75, 0x6a1936c8, 0x6ed82b7f,
  0x639b0da6, 0x675a1011, 0x791d4014, 0x7ddc5da3, 0x709f7b7a,
  0x745e66cd, 0x9823b6e0, 0x9ce2ab57, 0x91a18d8e, 0x95609039,
  0x8b27c03c, 0x8fe6dd8b, 0x82a5fb52, 0x8664e6e5, 0xbe2b5b58,
  0xbaea46ef, 0xb7a96036, 0xb3687d81, 0xad2f2d84, 0xa9ee3033,
  0xa4ad16ea, 0xa06c0b5d, 0xd4326d90, 0xd0f37027, 0xddb056fe,
  0xd9714b49, 0xc7361b4c, 0xc3f706fb, 0xceb42022, 0xca753d95,
  0xf23a8028, 0xf6fb9d9f, 0xfbb8bb46, 0xff79a6f1, 0xe13ef6f4,
  0xe5ffeb43, 0xe8bccd9a, 0xec7dd02d, 0x34867077, 0x30476dc0,
  0x3d044b19, 0x39c556ae, 0x278206ab, 0x23431b1c, 0x2e003dc5,
  0x2ac12072, 0x128e9dcf, 0x164f8078, 0x1b0ca6a1, 0x1fcdbb16,
  0x018aeb13, 0x054bf6a4, 0x0808d07d, 0x0cc9cdca, 0x7897ab07,
  0x7c56b6b0, 0x71159069, 0x75d48dde, 0x6b93dddb, 0x6f52c06c,
  0x6211e6b5, 0x66d0fb02, 0x5e9f46bf, 0x5a5e5b08, 0x571d7dd1,
  0x53dc6066, 0x4d9b3063, 0x495a2dd4, 0x44190b0d, 0x40d816ba,
  0xaca5c697, 0xa864db20, 0xa527fdf9, 0xa1e6e04e, 0xbfa1b04b,
  0xbb60adfc, 0xb6238b25, 0xb2e29692, 0x8aad2b2f, 0x8e6c3698,
  0x832f1041, 0x87ee0df6, 0x99a95df3, 0x9d684044, 0x902b669d,
  0x94ea7b2a, 0xe0b41de7, 0xe4750050, 0xe9362689, 0xedf73b3e,
  0xf3b06b3b, 0xf771768c, 0xfa325055, 0xfef34de2, 0xc6bcf05f,
  0xc27dede8, 0xcf3ecb31, 0xcbffd686, 0xd5b88683, 0xd1799b34,
  0xdc3abded, 0xd8fba05a, 0x690ce0ee, 0x6dcdfd59, 0x608edb80,
  0x644fc637, 0x7a089632, 0x7ec98b85, 0x738aad5c, 0x774bb0eb,
  0x4f040d56, 0x4bc510e1, 0x46863638, 0x42472b8f, 0x5c007b8a,
  0x58c1663d, 0x558240e4, 0x51435d53, 0x251d3b9e, 0x21dc2629,
  0x2c9f00f0, 0x285e1d47, 0x36194d42, 0x32d850f5, 0x3f9b762c,
  0x3b5a6b9b, 0x0315d626, 0x07d4cb91, 0x0a97ed48, 0x0e56f0ff,
  0x1011a0fa, 0x14d0bd4d, 0x19939b94, 0x1d528623, 0xf12f560e,
  0xf5ee4bb9, 0xf8ad6d60, 0xfc6c70d7, 0xe22b20d2, 0xe6ea3d65,
  0xeba91bbc, 0xef68060b, 0xd727bbb6, 0xd3e6a601, 0xdea580d8,
  0xda649d6f, 0xc423cd6a, 0xc0e2d0dd, 0xcda1f604, 0xc960ebb3,
  0xbd3e8d7e, 0xb9ff90c9, 0xb4bcb610, 0xb07daba7, 0xae3afba2,
  0xaafbe615, 0xa7b8c0cc, 0xa379dd7b, 0x9b3660c6, 0x9ff77d71,
  0x92b45ba8, 0x9675461f, 0x8832161a, 0x8cf30bad, 0x81b02d74,
  0x857130c3, 0x5d8a9099, 0x594b8d2e, 0x5408abf7, 0x50c9b640,
  0x4e8ee645, 0x4a4ffbf2, 0x470cdd2b, 0x43cdc09c, 0x7b827d21,
  0x7f436096, 0x7200464f, 0x76c15bf8, 0x68860bfd, 0x6c47164a,
  0x61043093, 0x65c52d24, 0x119b4be9, 0x155a565e, 0x18197087,
  0x1cd86d30, 0x029f3d35, 0x065e2082, 0x0b1d065b, 0x0fdc1bec,
  0x3793a651, 0x3352bbe6, 0x3e119d3f, 0x3ad08088, 0x2497d08d,
  0x2056cd3a, 0x2d15ebe3, 0x29d4f654, 0xc5a92679, 0xc1683bce,
  0xcc2b1d17, 0xc8ea00a0, 0xd6ad50a5, 0xd26c4d12, 0xdf2f6bcb,
  0xdbee767c, 0xe3a1cbc1, 0xe760d676, 0xea23f0af, 0xeee2ed18,
  0xf0a5bd1d, 0xf464a0aa, 0xf9278673, 0xfde69bc4, 0x89b8fd09,
  0x8d79e0be, 0x803ac667, 0x84fbdbd0, 0x9abc8bd5, 0x9e7d9662,
  0x933eb0bb, 0x97ffad0c, 0xafb010b1, 0xab710d06, 0xa6322bdf,
  0xa2f33668, 0xbcb4666d, 0xb8757bda, 0xb5365d03, 0xb1f740b4
};

uint32_t ChecksumBuffer(const char* buffer, unsigned length)
{
    unsigned i;
    uint32_t crc;
    
    // produces the same checksum as the GNU cksum command

    crc = 0;
    for (i = length; i--; )
        crc = (crc << 8) ^ crctab[((crc >> 24) ^ *buffer++) & 0xFF];
    for (i = length; i; i >>= 8)
        crc = (crc << 8) ^ crctab[((crc >> 24) ^ i) & 0xFF];
    crc = ~crc & 0xFFFFFFFF;
    
    return crc;
}

uint64_t ToLittle64(uint64_t num)
{
    return num;
}

uint32_t ToLittle32(uint32_t num)
{
    return num;
}

uint32_t ToLittle16(uint32_t num)
{
    return num;
}

uint64_t FromLittle64(uint64_t num)
{
    return num;
}

uint32_t FromLittle32(uint32_t num)
{
    return num;
}

uint32_t FromLittle16(uint32_t num)
{
    return num;
}

uint32_t NextPowerOfTwo(uint32_t x)
{
	x--;
	x |= x >> 1;
	x |= x >> 2;
	x |= x >> 4;
	x |= x >> 8;
	x |= x >> 16;
	x++;
	return x;
}
