#include "Version.h"
#include "System/Config.h"
#include "System/Events/EventLoop.h"
#include "System/IO/IOProcessor.h"
#include "Framework/StorageNew/StorageEnvironment.h"

#ifdef DEBUG
#define VERSION_FMT_STRING "ScalienDB v" VERSION_STRING " (DEBUG build date " __DATE__ " " __TIME__ ")"
#else
#define VERSION_FMT_STRING "ScalienDB v" VERSION_STRING
#endif

void InitLog();

int main()
{
    StartClock();
    IOProcessor::Init(configFile.GetIntValue("io.maxfd", 1024), true);
    
    EventLoop::Init();
    EventLoop::Run();
    EventLoop::Shutdown();
    
    
    IOProcessor::Shutdown();
    StopClock();
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
