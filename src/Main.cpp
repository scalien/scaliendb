#include "Version.h"
#include "System/Config.h"
#include "System/Events/EventLoop.h"
#include "System/IO/IOProcessor.h"
#include "Framework/Storage/BloomFilter.h"
#include "Application/Common/ContextTransport.h"
#include "Application/ConfigServer/ConfigServerApp.h"
#include "Application/ShardServer/ShardServerApp.h"

#define PRODUCT_STRING      "ScalienDB v" VERSION_STRING 

void InitLog();
void ParseArgs(int argc, char** argv);
void ConfigureSystemSettings();
bool IsController();
void InitContextTransport();
void LogPrintVersion(bool isController);

int main(int argc, char** argv)
{
    Application* app;
    bool isController;

    if (argc < 2)
        STOP_FAIL(1, "Config file argument not given, exiting");
        
    if (!configFile.Init(argv[1]))
        STOP_FAIL(1, "Invalid config file (%s)", argv[1]);

    InitLog();
    ParseArgs(argc, argv);
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
    
    app->Init();
    
    IOProcessor::BlockSignals(IOPROCESSOR_BLOCK_ALL);
    EventLoop::Init();
    EventLoop::Run();
    
    Log_Message("Shutting down...");
    
    EventLoop::Shutdown();
    app->Shutdown();
    delete app;
    
    IOProcessor::Shutdown();
    StopClock();
    configFile.Shutdown();
    Log_Shutdown();

    return 0;
}

void InitLog()
{
    int logTargets;

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
    Log_SetTimestamping(configFile.GetBoolValue("log.timestamping", false));
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
    const char*     debugInfo = "";
    const char*     productString = PRODUCT_STRING;

#ifdef DEBUG
    const char*     buildDate = "Build date: " __DATE__ " " __TIME__;
    Buffer          debugBuffer;
    
    debugBuffer.Appendf(" -- DEBUG %s, Pid: %U", buildDate, GetProcessID());
    debugBuffer.NullTerminate();
    debugInfo = debugBuffer.GetBuffer();
#endif
    
    Log_Message("%s started as %s%s", productString,
     isController ? "CONTROLLER" : "SHARD SERVER", debugInfo);
}
