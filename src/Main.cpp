#include "Version.h"
#include "System/Config.h"
#include "System/Events/EventLoop.h"
#include "System/IO/IOProcessor.h"
#include "Framework/Storage/StorageDataCache.h"
#include "Application/Common/ContextTransport.h"
#include "Application/Controller/ControllerApp.h"
#include "Application/ShardServer/ShardServerApp.h"

#ifdef DEBUG
#define VERSION_FMT_STRING "ScalienDB v" VERSION_STRING " (DEBUG build date " __DATE__ " " __TIME__ ")"
#else
#define VERSION_FMT_STRING "ScalienDB v" VERSION_STRING
#endif

void InitLog();
bool IsController();
void InitContextTransport();

int main(int argc, char** argv)
{
    Application*        app;
    bool                isController;

    if (argc < 2)
        STOP_FAIL(1, "Config file argument not given, exiting");
        
    if (!configFile.Init(argv[1]))
        STOP_FAIL(1, "Invalid config file (%s)", argv[1]);

    InitLog();
    StartClock();
    IOProcessor::Init(1024);
    InitContextTransport();
    
    isController = IsController();  
    Log_Message(VERSION_FMT_STRING " started as %s", isController ? "CONTROLLER" : "SHARD SERVER");
    if (isController)
        app = new ControllerApp;
    else
        app = new ShardServerApp;
    
    app->Init();
    
    EventLoop::Init();
    EventLoop::Run();
    EventLoop::Shutdown();
    
    app->Shutdown();
    delete app;
    
    IOProcessor::Shutdown();
    StopClock();
    configFile.Shutdown();
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

bool IsController()
{
    const char* role;
    
    role = configFile.GetValue("role", "");
    if (strcmp(role, "controller") == 0)
        return true;
    else
        return false;
}

void InitContextTransport()
{
    const char*     str;
    Endpoint        endpoint;

    // set my endpoint
    str = configFile.GetValue("endpoint", "");
    if (str == 0)
        ASSERT_FAIL();
    endpoint.Set(str);
    CONTEXT_TRANSPORT->Init(endpoint);
}
