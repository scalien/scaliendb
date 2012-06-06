#ifdef PLATFORM_WINDOWS
#include "CrashReporter.h"
#include "Threading/ThreadPool.h"
#include "Time.h"

#include <windows.h>
#include <dbghelp.h>
#include <stdio.h>

static Callable                 exceptionCallback;
static char                     exceptionMessage[16384];
static EXCEPTION_POINTERS*      exceptionPointers;
static volatile bool            exceptionActive;
static unsigned long            crashSleepTime; 
static ThreadPool*              crashThread;

#ifndef STATUS_HEAP_CORRUPTION
#define STATUS_HEAP_CORRUPTION  0xC0000374
#endif

static LONG CALLBACK VectoredExceptionHandler(EXCEPTION_POINTERS* pointers)
{
    // avoid infinite loops in exception handlers
    if (exceptionActive)
    {
        fprintf(stderr, "ExceptionActive");
        _exit(1);
    }

    // Setup the pointers
    exceptionPointers = pointers;
    
    // Unregister handler
    RemoveVectoredExceptionHandler(VectoredExceptionHandler);

    // Call user-provided callback
    Call(exceptionCallback);

    exceptionActive = true;

    // Usually exceptionCallback never returns. If it is, continue on other vectored handlers.
    return EXCEPTION_CONTINUE_SEARCH;
}

void CrashReporter::SetCallback(Callable callback)
{
    ULONG   firstHandler;

    if (exceptionCallback.IsSet())
        DeleteCallback();

    exceptionCallback = callback;
    exceptionMessage[0] = 0;
    exceptionActive = false;

    firstHandler = TRUE;
    AddVectoredExceptionHandler(firstHandler, VectoredExceptionHandler);
}

void CrashReporter::DeleteCallback()
{
    // Unregister handler
    RemoveVectoredExceptionHandler(VectoredExceptionHandler);
    
    exceptionCallback.Unset();
}

static const char* GetPossibleReason()
{
    const char* reason;

    reason = "Unknown";
    
    if (exceptionPointers->ExceptionRecord->ExceptionAddress == 0)
    {
        // The stack was filled with zeroes and function returned to zero address
        reason = "Call stack is filled with zeroes, stack is corrupted";
    }
    else if (exceptionPointers->ExceptionRecord->ExceptionCode == EXCEPTION_ACCESS_VIOLATION)
    {
        reason = "Access violation";
    }
    else if (exceptionPointers->ExceptionRecord->ExceptionCode == EXCEPTION_ILLEGAL_INSTRUCTION)
    {
        reason = "Illegal instruction";
    }
    else if (exceptionPointers->ExceptionRecord->ExceptionCode == EXCEPTION_STACK_OVERFLOW)
    {
        reason = "Stack overflow";
    }
    else if (exceptionPointers->ExceptionRecord->ExceptionCode == STATUS_HEAP_CORRUPTION)
    {
        reason = "Heap corruption (double free/delete?)";
    }

    return reason;
}

// No allocation is done in this function, because the heap may already be corrupted.
const char* CrashReporter::GetReport()
{
    char                symbolBuffer[sizeof(SYMBOL_INFO) + MAX_SYM_NAME * sizeof(TCHAR)];
    char                traceBuffer[8*1024];
    SYMBOL_INFO*        symbol;
    HANDLE              process;
    IMAGEHLP_LINE64     line;
    IMAGEHLP_MODULE64   module;
    DWORD               displacement;
    DWORD64             address;
    const char*         fileName;
    const char*         symbolName;
    const char*         reason;
    const char*         stackTrace;

    exceptionMessage[0] = 0;
    if (exceptionPointers == NULL)
        return exceptionMessage;

    process = GetCurrentProcess();
    SymInitialize(process, NULL, TRUE);
    SymSetOptions(SYMOPT_LOAD_LINES);

    symbol = (SYMBOL_INFO*) symbolBuffer;
    symbol->MaxNameLen = MAX_SYM_NAME;
    symbol->SizeOfStruct = sizeof(SYMBOL_INFO);

    line.SizeOfStruct = sizeof(IMAGEHLP_LINE64);
    line.LineNumber = 0;
    module.SizeOfStruct = sizeof(IMAGEHLP_MODULE64);

    address = (DWORD64) exceptionPointers->ExceptionRecord->ExceptionAddress;
    fileName = "UnknownFilename";
    symbolName = "UnknownSymbol";
    reason = "Unknown";

    do
    {
        if (SymFromAddr(process, address, 0, symbol) == FALSE)
            break;

        if (SymGetLineFromAddr64(process, address, &displacement, &line) == FALSE)
            break;

        if (SymGetModuleInfo64(process, address, &module) == FALSE)
            break;

        fileName = strrchr(module.ImageName, '\\');
        fileName = fileName ? fileName + 1 : module.ImageName;
        symbolName = symbol->Name;
    }
    while (false);

    // Heuristics for possible reasons
    reason = GetPossibleReason();

    // Retrieve stack trace
    stackTrace = GetStackTrace(traceBuffer, sizeof(traceBuffer), "\t");

    // Generate printable exception message
    snprintf(exceptionMessage, sizeof(exceptionMessage),
        "Critical error detected\n"
        "Exception: 0x%08X\n"
        "Reason: %s\n"
        "Address: 0x%p\n"
        "Location: %s!%s()  Line %u\n"
        "Stack trace: \n%s\n",
        exceptionPointers->ExceptionRecord->ExceptionCode,
        reason,
        exceptionPointers->ExceptionRecord->ExceptionAddress,
        fileName, symbolName, line.LineNumber,
        stackTrace);

    return exceptionMessage;
}

void CrashReporter::ReportSystemEvent(const char* ident)
{
    HANDLE  eventLog;
    BOOL    ret;
    WORD    category;
    DWORD   eventID;
    WORD    numStrings;
    DWORD   dataSize;
    LPCSTR  strings[1];

    eventLog = OpenEventLog(NULL, ident);
    if (eventLog == NULL)
    {
        Log_Errno("Cannot open event log!");
        return;
    }

    // this generates the report into exceptionMessage
    GetReport();

    category = 0;
    eventID = 0;
    numStrings = 1;
    strings[0] = exceptionMessage;
    dataSize = 0;
    ret = ReportEvent(eventLog, EVENTLOG_ERROR_TYPE, category, eventID, NULL, 
     numStrings, dataSize, strings, NULL);

    if (ret == FALSE)
        Log_Errno("Cannot report to event log!");

    ret = CloseEventLog(eventLog);
    if (ret == FALSE)
    {
        Log_Errno("Cannot close event log!");
        return;
    }
}

// Crash testing functionality support
void CrashThreadFunc()
{
    MSleep(crashSleepTime);
    
    Log_Message("Crashing...");
    Log_Shutdown();

    *((char*) 0) = 0;
}

// crashes the program by starting a thread that writes deliberately to an invalid address
// in the specified interval
void CrashReporter::TimedCrash(unsigned intervalMsec)
{
    if (crashThread != NULL)
        return;     // already running

    crashSleepTime = intervalMsec;
    crashThread = ThreadPool::Create(1);
    crashThread->Start();
    crashThread->Execute(CFunc(CrashThreadFunc));
}

void CrashReporter::RandomCrash(unsigned intervalMsec)
{
    TimedCrash(RandomInt(0, (int) intervalMsec));
}

#endif // PLATFORM_WINDOWS
