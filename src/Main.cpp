#include "Version.h"
#include "System/Config.h"
#include "System/Events/EventLoop.h"
#include "System/IO/IOProcessor.h"
#include "Application/Common/ContextTransport.h"
#include "Application/ConfigServer/ConfigServerApp.h"
#include "Application/ShardServer/ShardServerApp.h"

#define PRODUCT_STRING "ScalienDB v" VERSION_STRING 

void InitLog();
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
    StartClock();
    IOProcessor::Init(configFile.GetIntValue("io.maxfd", 1024));
    InitContextTransport();
    
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
    
    Log_Message("Shutdown initialized...");
    
    EventLoop::Shutdown();
    app->Shutdown();
    delete app;
    
    IOProcessor::Shutdown();
    DEFAULT_BUFFERPOOL->Shutdown();
    StopClock();
    configFile.Shutdown();
    Log_Shutdown();
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
    const char* str;
    Endpoint endpoint;

    // set my endpoint
    str = configFile.GetValue("endpoint", "");
    if (str == 0)
        ASSERT_FAIL();
    endpoint.Set(str);
    CONTEXT_TRANSPORT->Init(endpoint);
}

void LogPrintVersion(bool isController)
{
    const char*     debugInfo = "";
    const char*     productString = PRODUCT_STRING;
    const char*     buildDate = "Build date: " __DATE__ " " __TIME__;

#ifdef DEBUG
    Buffer          debugBuffer;
    
    debugBuffer.Appendf(" -- DEBUG %s, Pid: %U", buildDate, GetProcessID());
    debugInfo = debugBuffer.GetBuffer();
#endif    
    
    Log_Message("%s started as %s%s", productString,
     isController ? "CONTROLLER" : "SHARD SERVER", debugInfo);
}
