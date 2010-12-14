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
    Log_SetTarget(LOG_TARGET_STDOUT);
    Log_SetTrace(false);
    Log_SetTimestamping(true);
    EventLoop::Init();
    
    StorageEnvironment env;
    Buffer envPath;
    envPath.Write("db/");
    env.Open(envPath);
    env.CreateShard(1, 1, 1, ReadBuffer(""), ReadBuffer(""), true);
    
    Buffer key, value;
    uint64_t i;
    for (i = 0; i < 1000*1000; i++)
    {
        key.Writef("%U", i);
        value.Writef("%0100U", i);
        env.Set(1, 1, ReadBuffer(key), ReadBuffer(value));
        if (i % (100*1000) == 0)
        {
            env.Commit();
            Log_Message("%U", i);
        }
        if (i % (10*1000) == 0)
        {
            EventLoop::RunOnce();
        }
    }
    env.Commit();
    
    ReadBuffer rv;
    uint64_t rnd;
    Log_Message("GET begin");
    for (i = 0; i < 100*1000; i++)
    {
        rnd = RandomInt(0, 1000*1000 - 1);
        key.Writef("%U", rnd);
        if (!env.Get(1, 1, ReadBuffer(key), rv))
            Log_Message("%B => not found", &key);
    }
    
    Log_Message("GET end");
    env.Close();
    
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
