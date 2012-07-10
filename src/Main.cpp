#include "Version.h"
#include "SourceControl.h"
#include "System/Config.h"
#include "System/Events/EventLoop.h"
#include "System/IO/IOProcessor.h"
#include "System/Service.h"
#include "System/FileSystem.h"
#include "System/CrashReporter.h"
#include "Framework/Storage/BloomFilter.h"
#include "Application/Common/ContextTransport.h"
#include "Application/ConfigServer/ConfigServerApp.h"
#include "Application/ShardServer/ShardServerApp.h"
#ifdef _CRTDBG_MAP_ALLOC
#include <windows.h>
#endif

#define    IDENT            "ScalienDB"

const char PRODUCT_STRING[] = IDENT " v" VERSION_STRING " " PLATFORM_STRING;
const char BUILD_DATE[]     = "Build date: " __DATE__ " " __TIME__;

static void InitLog();
static void ParseArgs(int argc, char** argv);
static void SetupServiceIdentity(ServiceIdentity& ident);
static void RunMain(int argc, char** argv);
static void RunApplication();
static void ConfigureSystemSettings();
static bool IsController();
static void InitContextTransport();
static void LogPrintVersion(bool isController);
static void CrashReporterCallback();

// the application object is global for debugging purposes
static Application*     app;
static bool             restoreMode = false;
static bool             setNodeID = false;
static uint64_t         nodeID = 0;

int main(int argc, char** argv)
{
    SetMemoryLeakReports();

    try
    {
        // crash reporter messes up the debugging on Windows
#ifndef DEBUG
        CrashReporter::SetCallback(CFunc(CrashReporterCallback));
#endif
        RunMain(argc, argv);
    }
    catch (std::bad_alloc& e)
    {
        UNUSED(e);
        STOP_FAIL(1, "Out of memory error");
    }
    catch (std::exception& e)
    {
        STOP_FAIL(1, "Unexpected exception happened (%s)", e.what());
    }
    catch (...)
    {
        STOP_FAIL(1, "Unexpected exception happened");
    }

    ReportMemoryLeaks();

    return 0;
}

static void RunMain(int argc, char** argv)
{
    ServiceIdentity     identity;

    ParseArgs(argc, argv);

    if (argc < 2)
        STOP_FAIL(1, "Config file argument not given");
        
    if (!configFile.Init(argv[1]))
        STOP_FAIL(1, "Invalid config file (%s)", argv[1]);

    InitLog();
    
    // HACK: this is called twice, because command line arguments may override log settings
    ParseArgs(argc, argv);
    SetupServiceIdentity(identity);
    Service::Main(argc, argv, RunApplication, identity);
}

static void RunApplication()
{
    bool            isController;

    StartClock();
    ConfigureSystemSettings();
    
    IOProcessor::Init(configFile.GetIntValue("io.maxfd", 32768));
    InitContextTransport();
    BloomFilter::StaticInit();
    
    isController = IsController();
    LogPrintVersion(isController);
    if (isController)
        app = new ConfigServerApp(restoreMode);
    else
        app = new ShardServerApp(restoreMode, setNodeID, nodeID);
    
    Service::SetStatus(SERVICE_STATUS_RUNNING);
    app->Init();
    
    IOProcessor::BlockSignals(IOPROCESSOR_BLOCK_ALL);
    EventLoop::Init();
    
    EventLoop::Run();
    
    Service::SetStatus(SERVICE_STATUS_STOP_PENDING);
    Log_Message("Shutting down...");
    
    EventLoop::Shutdown();
    app->Shutdown();
    delete app;
    
    IOProcessor::Shutdown();
    StopClock();
    configFile.Shutdown();
    Registry::Shutdown();

    // This is here and not the end of main(), because logging may be turned off by then
    Log_Message(IDENT " exited normally");
    Log_Shutdown();
}

static void SetupServiceIdentity(ServiceIdentity& identity)
{
    // set up service identity based on role
    if (IsController())
    {
        identity.name = "ScalienController";
        identity.displayName = "Scalien Database Controller";
        identity.description = "Provides and stores metadata for Scalien Database cluster";
    }
    else
    {
        identity.name = "ScalienShardServer";
        identity.displayName = "Scalien Database Shard Server";
        identity.description = "Provides reliable and replicated data storage for Scalien Database cluster";
    }
}

static void InitLog()
{
    int logTargets;
    bool debug;

#ifdef DEBUG
    debug = true;
#else
    debug = false;
#endif

    logTargets = 0;
    if (configFile.GetListNum("log.targets") == 0)
        logTargets = LOG_TARGET_STDOUT;

    for (int i = 0; i < configFile.GetListNum("log.targets"); i++)
    {
        if (strcmp(configFile.GetListValue("log.targets", i, ""), "file") == 0)
        {
            logTargets |= LOG_TARGET_FILE;
            Log_SetOutputFile(configFile.GetValue("log.file", NULL),
            configFile.GetBoolValue("log.truncate", false));
        }
        if (strcmp(configFile.GetListValue("log.targets", i, NULL), "stdout") == 0)
            logTargets |= LOG_TARGET_STDOUT;
        if (strcmp(configFile.GetListValue("log.targets", i, NULL), "stderr") == 0)
            logTargets |= LOG_TARGET_STDERR;
    }
    Log_SetTarget(logTargets);

    Log_SetTrace(configFile.GetBoolValue("log.trace", false));
    Log_SetDebug(configFile.GetBoolValue("log.debug", debug));
    Log_SetTimestamping(configFile.GetBoolValue("log.timestamping", false));
    Log_SetAutoFlush(configFile.GetBoolValue("log.autoFlush", true));
    Log_SetMaxSize(configFile.GetIntValue("log.maxSize", 100*1000*1000) / (1000 * 1000));
    Log_SetTraceBufferSize(configFile.GetIntValue("log.traceBufferSize", 0));
    Log_SetFlushInterval(configFile.GetIntValue("log.flushInterval", 0) * 1000);
}

static void ParseDebugArgs(char* arg)
{
    bool    pause = false;

    switch (arg[0])
    {
    case 'X':
        // Do not exit on error or assert
        SetExitOnError(false);
        SetAssertCritical(false);
        break;
    case 'P':
        // Pause execution while debugger is attaching
        // Once the debugger is attached, change the value of pause to false
        pause = true;
        while (pause)
            MSleep(1000);   // <- Put debugger breakpoint this line
        break;
    }
}

static void ParseArgs(int argc, char** argv)
{
    ReadBuffer arg;


    for (int i = 1; i < argc; i++)
    {
        if (argv[i][0] == '-')
        {
            switch (argv[i][1])
            {
            case 't':
                Log_SetTrace(true);
                break;
            case 'D':
                // Debugging options
                ParseDebugArgs(&argv[i][2]);
                break;
            case 'v':
                STOP("%s", PRODUCT_STRING);
                break;
            case 'r':
                restoreMode = true;
                break;
            case 'n':
                setNodeID = true;
                i++;
                arg.Wrap(argv[i]);
                arg.Readf("%U", &nodeID);
                break;
            case 'h':
                STOP("Usage:\n"
                     "\n"
                     "       %s config-file [options] [service-options]\n"
                     "\n"
                     "Options:\n"
                     "\n"
                     "       -v: print version number and exit\n"
                     "       -r: start server in restore mode\n"
                     "       -t: turn trace mode on\n"
                     "       -h: print this help\n"
                     "\n"
                     "Service options (mutually exclusive):\n"
                     "\n"
                     "       --install:   install service\n"
                     "       --reinstall: reinstall service\n"
                     "       --uninstall: uninstall server\n"
                     , argv[0]);
                break;
            }
        }
    }
}

static void ConfigureSystemSettings()
{
    int         memLimitPerc;
    uint64_t    memLimit;
    const char* dir;

    // percentage of physical memory can be used by the program
    memLimitPerc = configFile.GetIntValue("system.memoryLimitPercentage", 90);
    if (memLimitPerc < 0)
        memLimitPerc = 90;
    
    // memoryLimit overrides memoryLimitPercentage
    memLimit = configFile.GetInt64Value("system.memoryLimit", 0);
    if (memLimit == 0)
        memLimit = (uint64_t) (GetTotalPhysicalMemory() * memLimit / 100.0 + 0.5);

    if (memLimit != 0)
        SetMemoryLimit(memLimit);

    // set the base directory
    dir = configFile.GetValue("dir", NULL);
    if (dir)
    {
        if (!FS_ChangeDir(dir))
            STOP_FAIL(1, "Cannot change to dir: %s", dir);
        
        // setting the base dir may affect the location of the log file
        InitLog();
    }

    // set exit on error: this is how ASSERTs are handled in release build
    SetExitOnError(true);

    // this must be called here, because the first value it returns might be unreliable
    GetTotalCpuUsage();

    SeedRandom();
}

static bool IsController()
{
    const char* role;
    
    role = configFile.GetValue("role", "");
    if (role == NULL)
        STOP_FAIL(1, "Missing \"role\" in config file!");
        
    if (strcmp(role, "controller") == 0)
        return true;
    else
        return false;
}

static void InitContextTransport()
{
    const char* str;
    Endpoint    endpoint;

    // set my endpoint
    str = configFile.GetValue("endpoint", "");
    if (str == NULL)
        STOP_FAIL(1, "Missing \"endpoint\" in config file!");

    if (!endpoint.Set(str, true))
        STOP_FAIL(1, "Bad endpoint format in config file!");

    CONTEXT_TRANSPORT->Init(endpoint);
}

static void LogPrintVersion(bool isController)
{
    Log_Message("%s started as %s", PRODUCT_STRING,
     isController ? "CONTROLLER" : "SHARD SERVER");

    Log_Message("Pid: %U", GetProcessID());
    Log_Message("%s", BUILD_DATE);
    Log_Message("Branch: %s", SOURCE_CONTROL_BRANCH);
    Log_Message("Source control version: %s", SOURCE_CONTROL_VERSION);
    Log_Message("================================================================");
}

static void CrashReporterCallback()
{
    const char*     msg;
    
    // We need to be careful here, because by the time the control gets here the stack and the heap
    // may already be corrupted. Therefore there must not be any heap allocations here, but unfortunately
    // stack allocations cannot be avoided.

    // When rotating the log there is heap allocation. To prevent it, we turn off log rotation ASAP.
    Log_SetMaxSize(0);

    CrashReporter::ReportSystemEvent(IDENT);

    // Generate report and send it to log and standard error
    msg = CrashReporter::GetReport();

    Log_SetTarget(Log_GetTarget() | LOG_TARGET_STDERR | LOG_TARGET_FILE);
    Log_Message("%s", msg);
    IFDEBUG(ASSERT_FAIL());
    _exit(1);
}
