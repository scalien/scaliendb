//#include "Version.h"
//#include "System/Config.h"
//#include "Framework/Database/Database.h"
//#include "Framework/Replication/ReplicationManager.h"
//#include "Application/Controller/ControlConfigContext.h"
//#include "Application/Controller/Controller.h"
//#include "Application/DataNode/DataNode.h"
//#include "Application/DataNode/DataChunkContext.h"
//#include "Application/HTTP/HTTPServer.h"
//
//#ifdef DEBUG
//#define VERSION_FMT_STRING "ScalienDB v" VERSION_STRING " (DEBUG build date " __DATE__ " " __TIME__ ")"
//#else
//#define VERSION_FMT_STRING "ScalienDB v" VERSION_STRING 
//#endif
//
//void InitLog()
//{
//	int logTargets;
//	
//	logTargets = 0;
//	if (configFile.GetListNum("log.targets") == 0)
//		logTargets = LOG_TARGET_STDOUT;
//	for (int i = 0; i < configFile.GetListNum("log.targets"); i++)
//	{
//		if (strcmp(configFile.GetListValue("log.targets", i, ""), "file") == 0)
//		{
//			logTargets |= LOG_TARGET_FILE;
//			Log_SetOutputFile(configFile.GetValue("log.file", NULL), 
//							configFile.GetBoolValue("log.truncate", false));
//		}
//		if (strcmp(configFile.GetListValue("log.targets", i, NULL), "stdout") == 0)
//			logTargets |= LOG_TARGET_STDOUT;
//		if (strcmp(configFile.GetListValue("log.targets", i, NULL), "stderr") == 0)
//			logTargets |= LOG_TARGET_STDERR;
//	}
//	Log_SetTarget(logTargets);
//	Log_SetTrace(configFile.GetBoolValue("log.trace", false));
//	Log_SetTimestamping(configFile.GetBoolValue("log.timestamping", false));
//}
//

#include "Framework/Storage/StorageShard.h"
#include "System/Stopwatch.h"
#include "System/Containers/InTreeMap.h"
#include "System/Events/Callable.h"
#include "Test/Test.h"

#include <map>
#include <stdlib.h>
#include <stdio.h>


TEST_DECLARE(TestMain);

int main(int argc, char** argv)
{
#define PRINT()			{v.Write(rv); v.NullTerminate(); k.NullTerminate(); printf("%s => %s\n", k.GetBuffer(), v.GetBuffer());}
	StorageShard		catalog;
	StorageCursor*		cursor;
	Buffer				k, v;
	ReadBuffer			rk, rv;
	Stopwatch			sw;
	long				elapsed;
	unsigned			num, len, ksize, vsize;
	char*				area;
	char*				p;
	uint64_t			clock;
	Deferred			stopClock(CFunc(StopClock));

	Log_SetTarget(LOG_TARGET_STDOUT);
	Log_SetTrace(true);
	StartClock();
#define TEST_FILE

#ifdef TEST_FILE
	return TestMain();
#endif

	num = 100*1000;
	ksize = 20;
	vsize = 128;
	area = (char*) malloc(num*(ksize+vsize));

	catalog.Open("dogs");

//	clock = NowClock();
//	sw.Start();
//	for (int i = 0; i < num; i++)
//	{
//		p = area + i*(ksize+vsize);
//		len = snprintf(p, ksize, "%d", i);
//		rk.SetBuffer(p);
//		rk.SetLength(len);
//		p += ksize;
//		len = snprintf(p, vsize, "%.100f", (float) i);
//		rv.SetBuffer(p);
//		rv.SetLength(len);
//		catalog.Set(rk, rv, false);
//
//		if (NowClock() - clock >= 1000)
//		{
//			printf("syncing...\n");
//			catalog.Flush();
//			clock = NowClock();
//		}
//	}
//	elapsed = sw.Stop();
//	printf("%u sets took %ld msec\n", num, elapsed);

//	k.Clear();
//	rk.Wrap(k);
//	cursor = new StorageCursor(&catalog);
//	StorageKeyValue* kv = cursor->Begin(rk);
//	while (kv != NULL)
//	{
//		printf("%.*s => %.*s\n", kv->key.GetLength(), kv->key.GetBuffer(), kv->value.GetLength(), kv->value.GetBuffer());
//		kv = cursor->Next();
//	}
//	cursor->Close();

//	sw.Reset();
//	sw.Start();
//	for (int i = 0; i < num; i++)
//	{
//		k.Writef("%d", i);
//		rk.Wrap(k);
//		if (catalog.Get(rk, rv))
//			PRINT()
//		else
//			ASSERT_FAIL();
//	}	
//	elapsed = sw.Stop();
//	printf("%u gets took %ld msec\n", num, elapsed);

	// test COW cursors
	
	k.Clear();
	rk.Wrap(k);
	cursor = new StorageCursor(&catalog);
	StorageKeyValue* kv = cursor->Begin(rk);
	while (kv != NULL)
	{
		printf("%.*s => %.*s\n", kv->key.GetLength(), kv->key.GetBuffer(), kv->value.GetLength(), kv->value.GetBuffer());
		catalog.Delete(rk);
		kv = cursor->Next();
		break;
	}
	cursor->Close();
	

//	sw.Reset();
//	sw.Start();
//	for (int i = 0; i < num; i++)
//	{
//		k.Writef("%d", i);
//		rk.Wrap(k);
//		catalog.Delete(rk); 
//	}	
//	elapsed = sw.Stop();
//	printf("%u deletes took %ld msec\n", num, elapsed);
	
	sw.Reset();
	sw.Start();
	catalog.Commit();
	catalog.Close();
	elapsed = sw.Stop();
	printf("Close() took %ld msec\n", elapsed);
							
//	DatabaseConfig				dbConfig;
//	Database					db;
//	ControlConfigContext		configContext;
//	DataChunkContext			dataContext;
//	Controller					controller;
//	DataNode					dataNode;
//	SingleQuorum				singleQuorum;
//	QuorumDatabase				qdb;
//	QuorumTransport				configTransport;
//	QuorumTransport				chunkTransport;
//	Buffer						prefix;
//	HTTPServer					httpServer;
//	int							httpPort;
//	Endpoint					endpoint;
//	uint64_t					nodeID;
//	uint64_t					runID;
//	bool						isController;
//	const char*					role;
//	Table*						table;
//	
//	if (argc < 2)
//		STOP_FAIL(1, "usage: %s config-file\n", argv[0]);
//
//	if (!configFile.Init(argv[1]))
//		STOP_FAIL(1, "cannot open config file! (%s)", argv[1]);
//	role = configFile.GetValue("role", "data");
//	if (strcmp(role, "data") == 0)
//		isController = false;
//	else
//		isController = true;
//
//	InitLog();
//	Log_Message(VERSION_FMT_STRING " started as %s", isController ? "CONTROLLER" : "DATA");
//
//	SeedRandom();
//	IOProcessor::Init(1024);
//
//	Log_Message("Opening database...");
//	dbConfig.dir = configFile.GetValue("database.dir", DATABASE_CONFIG_DIR);
//	db.Init(dbConfig);
//
//	Log_Message("Free disk space is %s", HumanBytes(GetFreeDiskSpace("/")));
//
//	// Initialize runID
//	runID = 0;
//	table = db.GetTable("system");
//	table->Get(NULL, "#runID", runID);
//	runID++;
//	table->Set(NULL, "#runID", runID);
//	RMAN->SetRunID(runID);
//	if (runID == 1)
//		Log_Message("Database empty, first run");
//	else
//		Log_Message("Database opened, %u. run", (unsigned) runID);
//
//
//	if (isController)
//	{
//		nodeID = configFile.GetIntValue("nodeID", 0);
//		RMAN->SetNodeID(nodeID);
//		const char* s = configFile.GetListValue("controllers", RMAN->GetNodeID(), NULL);
//		endpoint.Set(s);
//		RMAN->GetTransport()->Init(RMAN->GetNodeID(), endpoint);
//	}
//	else
//	{
//		dataNode.Init(db.GetTable("chunk1")); // sets RMAN->nodeID
//		const char* s = configFile.GetValue("endpoint", NULL);
//		endpoint.Set(s);
//		RMAN->GetTransport()->Init(RMAN->GetNodeID(), endpoint);
//	}
//	
//	unsigned numNodes = configFile.GetListNum("controllers");
//	for (unsigned i = 0; i < numNodes; i++)
//	{
//		const char* s = configFile.GetListValue("controllers", i, NULL);
//		endpoint.Set(s);
//
//		// TODO: i will not be a nodeID once we start using UIDs
//		RMAN->GetTransport()->AddEndpoint(i, endpoint);
//
//		if (isController)
//			singleQuorum.AddNode(i);
//	}
//
//	if (isController)
//	{
//		qdb.SetSection(db.GetTable("chunk0"));
//		qdb.SetTransaction(NULL);
//		prefix.Write("0");
//		configTransport.SetPriority(true);
//		configTransport.SetPrefix(prefix);
//		configTransport.SetQuorum(&singleQuorum);
//		
//		configContext.SetContextID(0);
//		configContext.SetController(&controller);
//		configContext.SetQuorum(singleQuorum);
//		configContext.SetDatabase(qdb);
//		configContext.SetTransport(configTransport);
//		configContext.Start();
//
//		controller.Init(db.GetTable("chunk0"));
//		RMAN->GetTransport()->SetController(&controller); // TODO: hack
//		RMAN->AddContext(&configContext);
//
//		httpPort = configFile.GetIntValue("http.port", 8080);
//		Log_Message("HTTP Server listening on port %d", httpPort);
//		httpServer.Init(httpPort);
//		httpServer.RegisterHandler(&controller);
//	}
//	else
//	{
//		qdb.SetSection(db.GetTable("chunk1"));
//		qdb.SetTransaction(NULL);
//		prefix.Write("1");
//		chunkTransport.SetPriority(true);
//		chunkTransport.SetPrefix(prefix);
//		chunkTransport.SetQuorum(dataContext.GetQuorum());	
//
//		dataContext.SetContextID(1);
//		dataContext.SetDatabase(qdb); // TODO: hack
//		dataContext.SetTransport(chunkTransport);
//		dataContext.Start(&dataNode);
//
//		httpPort = configFile.GetIntValue("http.port", 8080);
//		Log_Message("HTTP Server listening on port %d", httpPort);
//		httpServer.Init(httpPort);
//		httpServer.RegisterHandler(dataNode.GetHandler());
//		RMAN->AddContext(&dataContext);
//	}
//
//	Log_Message("Running, NodeID: %u", (unsigned) RMAN->GetNodeID());
//	
//	EventLoop::Init();
//	EventLoop::Run();
//	EventLoop::Shutdown();
}
