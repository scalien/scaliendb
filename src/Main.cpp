#include "Version.h"
#include "SourceControl.h"
#include "System/Config.h"
#include "System/Events/EventLoop.h"
#include "System/IO/IOProcessor.h"
#include "System/Service.h"
#include "System/FileSystem.h"
#include "Framework/Storage/BloomFilter.h"
#include "Application/Common/ContextTransport.h"
#include "Application/ConfigServer/ConfigServerApp.h"
#include "Application/ShardServer/ShardServerApp.h"

const char PRODUCT_STRING[] = "ScalienDB v" VERSION_STRING " " PLATFORM_STRING;
const char BUILD_DATE[]     = "Build date: " __DATE__ " " __TIME__;

void InitLog();
void ParseArgs(int argc, char** argv);
void RunMain(int argc, char** argv);
void RunApplication();
void ConfigureSystemSettings();
bool IsController();
void InitContextTransport();
void LogPrintVersion(bool isController);

int main(int argc, char** argv)
{
    try
    {
        RunMain(argc, argv);
    }
    catch (std::bad_alloc&)
    {
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

    return 0;
}

void RunMain(int argc, char** argv)
{
    if (argc < 2)
        STOP_FAIL(1, "Config file argument not given");
        
    if (!configFile.Init(argv[1]))
        STOP_FAIL(1, "Invalid config file (%s)", argv[1]);

    InitLog();
    ParseArgs(argc, argv);
    Service::Main(argc, argv, RunApplication);
}

void RunApplication()
{
    Application* app;
    bool isController;

    StartClock();
    ConfigureSystemSettings();
    
    IOProcessor::Init(configFile.GetIntValue("io.maxfd", 32768));
    InitContextTransport();
    BloomFilter::StaticInit();
    
    isController = IsController();
    LogPrintVersion(isController);
    if (isController)
        app = new ConfigServerApp;
    else
        app = new ShardServerApp;
    
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
    Log_Shutdown();
}

void InitLog()
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
}

void ParseArgs(int argc, char** argv)
{
    for (int i = 1; i < argc; i++)
    {
        if (argv[i][0] == '-')
        {
            switch (argv[i][1])
            {
            case 't':
                Log_SetTrace(true);
                break;
            case 'h':
                STOP_FAIL(0, "Usage: %s [-t] config-file\n\n", argv[0]);
                break;
            }
        }
    }
}

void ConfigureSystemSettings()
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
}

bool IsController()
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

void InitContextTransport()
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

void LogPrintVersion(bool isController)
{
    Log_Message("%s started as %s", PRODUCT_STRING,
     isController ? "CONTROLLER" : "SHARD SERVER");

    Log_Debug("Pid: %U", GetProcessID());
    Log_Debug("%s", BUILD_DATE);
    Log_Debug("Branch: %s", SOURCE_CONTROL_BRANCH);
    Log_Debug("Source control version: %s", SOURCE_CONTROL_VERSION);
    Log_Debug("================================================================");
}
