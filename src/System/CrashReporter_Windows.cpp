#ifdef PLATFORM_WINDOWS
#include "CrashReporter.h"

#include <windows.h>
#include <dbghelp.h>
#include <stdio.h>

static Callable                 exceptionCallback;
static char                     exceptionMessage[16384];
static EXCEPTION_POINTERS*      exceptionPointers;

#ifndef STATUS_HEAP_CORRUPTION
#define STATUS_HEAP_CORRUPTION  0xC0000374
#endif

static LONG CALLBACK VectoredExceptionHandler(EXCEPTION_POINTERS* pointers)
{
    // Setup the pointers
    exceptionPointers = pointers;
    
    // Call user-provided callback
    Call(exceptionCallback);

    // Usually exceptionCallback never returns. If it is, continue on other vectored handlers.
    return EXCEPTION_CONTINUE_SEARCH;
}

void CrashReporter::SetCallback(Callable callback)
{
    ULONG   firstHandler;

    exceptionCallback = callback;
    exceptionMessage[0] = 0;

    firstHandler = TRUE;
    AddVectoredExceptionHandler(firstHandler, VectoredExceptionHandler);
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
    SYMBOL_INFO*        symbol;
    HANDLE              process;
    IMAGEHLP_LINE64     line;
    IMAGEHLP_MODULE64   module;
    DWORD               displacement;
    DWORD64             address;
    const char*         fileName;
    const char*         symbolName;
    const char*         reason;

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

    // Generate printable exception message
    snprintf(exceptionMessage, sizeof(exceptionMessage),
        "Critical error detected\n"
        "Exception: 0x%08X\n"
        "Address: 0x%p\n"
        "Location: %s!%s()  Line %u\n"
        "Reason: %s\n",
        exceptionPointers->ExceptionRecord->ExceptionCode,
        exceptionPointers->ExceptionRecord->ExceptionAddress,
        fileName, symbolName, line.LineNumber,
        reason);

    return exceptionMessage;
}

#ifdef DEBUG
// Crash testing functionality support. Should NOT be included in release versions!
#include "Threading/ThreadPool.h"
#include "Time.h"

static unsigned long    crashSleepTime; 
static ThreadPool*      crashThread;

void CrashThreadFunc()
{
    MSleep(crashSleepTime);
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

#endif // DEBUG

#endif // PLATFORM_WINDOWS
