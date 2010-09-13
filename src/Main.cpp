#include "Version.h"
#include "System/Config.h"
#include "System/Events/EventLoop.h"
#include "System/IO/IOProcessor.h"
#include "Application/Controller/Controller.h"
#include "Application/ShardServer/ShardServer.h"

#ifdef DEBUG
#define VERSION_FMT_STRING "ScalienDB v" VERSION_STRING " (DEBUG build date " __DATE__ " " __TIME__ ")"
#else
#define VERSION_FMT_STRING "ScalienDB v" VERSION_STRING
#endif

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
	
	if (strcmp(role, "shard") == 0)
		return false;
	else
		return true;
}

int main(int argc, char** argv)
{
	Controller*			controller;
	ShardServer*		shardServer;
	bool				isController;
	
	if (argc < 2)
		ASSERT_FAIL();
		
	configFile.Init(argv[1]);
	
	InitLog();

	isController = IsController();
	
	Log_Message(VERSION_FMT_STRING " started as %s", isController ? "CONTROLLER" : "DATA");

	IOProcessor::Init(1024);
	
	if (isController)
	{
		controller = new Controller;
		controller->Init();
	}
	else
	{
		shardServer = new ShardServer;
		shardServer->Init();
	}
	
	EventLoop::Init();
	EventLoop::Run();
	EventLoop::Shutdown();
}
