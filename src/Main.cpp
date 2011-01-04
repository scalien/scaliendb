#include "Version.h"
#include "System/Config.h"
#include "System/Events/EventLoop.h"
#include "System/IO/IOProcessor.h"
#include "Framework/Storage/StorageEnvironment.h"

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
    env.CreateShard(1, 2, 1, ReadBuffer(""), ReadBuffer(""), true);

    Buffer key, value;
    ReadBuffer rk, rv;
    uint64_t i,j, rnd, num;

    key.Writef("999999");
    value.Writef("world");

    env.Get(1, 2, ReadBuffer(key), rv);

    env.Set(1, 2, ReadBuffer(key), ReadBuffer(value));
    
    num = 10*1000*1000;
    for (i = 0; i < num; i++)
    {
// rnd = RandomInt(0, num - 1);
        key.Writef("%U", i);
        value.Writef("%0100U", i);
        env.Set(1, 1, ReadBuffer(key), ReadBuffer(value));
        if (i % (100*1000) == 0)
        {
            env.Commit();
            Log_Message("%U", i);
        }
        if (i > 0 && i % (10*1000) == 0)
        {
            EventLoop::RunOnce();
            
//            for (j = 0; j < 1000; j++)
//            {
//                rnd = RandomInt(0, i - 1);
//                key.Writef("%U", rnd);
//                if (!env.Get(1, 1, ReadBuffer(key), rv))
//                    Log_Message("%B => not found", &key);
//            }
        }
    }
    env.Commit();
    
    StorageBulkCursor* bc = env.GetBulkCursor(1, 1);
    StorageKeyValue* it;
    i = 0;
    Log_Message("Bulk iterating...");
    FOREACH(it, *bc)
    {
        rk = it->GetKey();
        rv = it->GetValue();
//        Log_Message("%R => %R", &rk, &rv);
        i++;
        if (i > 0 && i % (100*1000) == 0)
        {
            Log_Message("%U", i);
            Log_Message("%R => %R", &rk, &rv);
        }

    }
    Log_Message("%d", i);

    
// ReadBuffer rv;
// uint64_t rnd;
// Log_Message("GET begin");
// for (i = 0; i < 100*1000; i++)
// {
// rnd = RandomInt(0, 1000*1000 - 1);
// key.Writef("%U", rnd);
// if (!env.Get(1, 1, ReadBuffer(key), rv))
// Log_Message("%B => not found", &key);
// }
//
// Log_Message("GET end");
// env.Close();
    
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

//#include "Version.h"
//#include "System/Config.h"
//#include "System/Events/EventLoop.h"
//#include "System/IO/IOProcessor.h"
//#include "Application/Common/ContextTransport.h"
//#include "Application/ConfigServer/ConfigServerApp.h"
//#include "Application/ShardServer/ShardServerApp.h"
//
//#ifdef DEBUG
//#define VERSION_FMT_STRING "ScalienDB v" VERSION_STRING " (DEBUG build date " __DATE__ " " __TIME__ ")"
//#else
//#define VERSION_FMT_STRING "ScalienDB v" VERSION_STRING
//#endif
//
//void InitLog();
//bool IsController();
//void InitContextTransport();
//
//int main(int argc, char** argv)
//{
//    Application*        app;
//    bool                isController;
//
//    if (argc < 2)
//        STOP_FAIL(1, "Config file argument not given, exiting");
//        
//    if (!configFile.Init(argv[1]))
//        STOP_FAIL(1, "Invalid config file (%s)", argv[1]);
//
//    InitLog();
//    StartClock();
//    IOProcessor::Init(configFile.GetIntValue("io.maxfd", 1024), true);
//    InitContextTransport();
//    
//    isController = IsController();  
//    Log_Message(VERSION_FMT_STRING " started as %s", isController ? "CONTROLLER" : "SHARD SERVER");
//    if (isController)
//        app = new ConfigServerApp;
//    else
//        app = new ShardServerApp;
//    
//    app->Init();
//    
//    EventLoop::Init();
//    EventLoop::Run();
//    EventLoop::Shutdown();
//    
//    app->Shutdown();
//    delete app;
//    
//    IOProcessor::Shutdown();
//    DEFAULT_BUFFERPOOL->Shutdown();
//    StopClock();
//    configFile.Shutdown();
//    Log_Shutdown();
//}
//
//void InitLog()
//{
//    int logTargets;
//
//    logTargets = 0;
//    if (configFile.GetListNum("log.targets") == 0)
//        logTargets = LOG_TARGET_STDOUT;
//
//    for (int i = 0; i < configFile.GetListNum("log.targets"); i++)
//    {
//        if (strcmp(configFile.GetListValue("log.targets", i, ""), "file") == 0)
//        {
//            logTargets |= LOG_TARGET_FILE;
//            Log_SetOutputFile(configFile.GetValue("log.file", NULL),
//            configFile.GetBoolValue("log.truncate", false));
//        }
//        if (strcmp(configFile.GetListValue("log.targets", i, NULL), "stdout") == 0)
//            logTargets |= LOG_TARGET_STDOUT;
//        if (strcmp(configFile.GetListValue("log.targets", i, NULL), "stderr") == 0)
//            logTargets |= LOG_TARGET_STDERR;
//    }
//    Log_SetTarget(logTargets);
//
//    Log_SetTrace(configFile.GetBoolValue("log.trace", false));
//    Log_SetTimestamping(configFile.GetBoolValue("log.timestamping", false));
//}
//
//bool IsController()
//{
//    const char* role;
//    
//    role = configFile.GetValue("role", "");
//    if (strcmp(role, "controller") == 0)
//        return true;
//    else
//        return false;
//}
//
//void InitContextTransport()
//{
//    const char*     str;
//    Endpoint        endpoint;
//
//    // set my endpoint
//    str = configFile.GetValue("endpoint", "");
//    if (str == 0)
//        ASSERT_FAIL();
//    endpoint.Set(str);
//    CONTEXT_TRANSPORT->Init(endpoint);
//}
