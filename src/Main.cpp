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

#include "Framework/Storage/Catalog.h"
#include "System/Stopwatch.h"
#include "stdio.h"
#include "System/Containers/InTreeMap.h"


static int KeyCmp(ReadBuffer a, ReadBuffer b)
{
	// TODO: this is ineffective
	if (ReadBuffer::LessThan(a, b))
		return -1;
	if (ReadBuffer::LessThan(b, a))
		return 1;
	return 0;
}

static ReadBuffer Key(KeyValue* kv)
{
	return kv->key;
}

int TestTreeMap()
{
	InTreeMap<KeyValue, &KeyValue::node>	kvs;
	ReadBuffer								rb;
	Buffer									buf;
	KeyValue*								kv;
	KeyValue*								it;
	Stopwatch								sw;
	const unsigned							num = 100000;
	
	sw.Start();
	for (unsigned u = 0; u < num; u++)
	{
		buf.Writef("%u", u);
		rb.Wrap(buf);
		kv = new KeyValue;
		kv->SetKey(rb, true);
		kv->SetValue(rb, true);
		
		kvs.Insert(kv);
	}
	sw.Stop();
	printf("insert time: %ld\n", sw.Elapsed());

	sw.Reset();
	sw.Start();
	for (unsigned u = 0; u < num; u++)
	{
		buf.Writef("%u", u);
		rb.Wrap(buf);
		kv = kvs.Get<ReadBuffer>(rb);
		if (kv == NULL)
			ASSERT_FAIL();
	}
	sw.Stop();		
	printf("get time: %ld\n", sw.Elapsed());

	sw.Reset();
	sw.Start();
	for (it = kvs.First(); it != NULL; it = kvs.Next(it))
	{
		//printf("it->value = %.*s\n", P(&it->value));
		kv = it; // dummy op
	}
	sw.Stop();		
	printf("iteration time: %ld\n", sw.Elapsed());


	sw.Reset();
	sw.Start();
	//for (unsigned u = 0; u < num; u++)
	for (unsigned u = num - 1; u < num; u--)
	{
		buf.Writef("%u", u);
		rb.Wrap(buf);
		kv = kvs.Delete<ReadBuffer>(rb);
		if (kv == NULL)
			ASSERT_FAIL();
	}
	sw.Stop();		
	printf("delete time: %ld\n", sw.Elapsed());

	sw.Reset();
	sw.Start();
	for (unsigned u = 0; u < num; u++)
	{
		buf.Writef("%u", u);
		rb.Wrap(buf);
		kv = kvs.Get<ReadBuffer>(rb);
		if (kv != NULL)
			ASSERT_FAIL();
	}
	sw.Stop();
	printf("get time: %ld\n", sw.Elapsed());

	return 0;
}

int main(int argc, char** argv)
{
#define PRINT()			{v.Write(rv); v.NullTerminate(); k.NullTerminate(); printf("%s => %s\n", k.GetBuffer(), v.GetBuffer());}
//	File		file;
	Catalog		catalog;
	Buffer		k, v;
	ReadBuffer	rk, rv;
	Stopwatch	sw;
	long		elapsed;
	unsigned	num, len, ksize, vsize;
	char*		area;
	char*		p;
	
//	return TestTreeMap();
	
//	W(k, rk, "ki");
//	W(v, rv, "atka");
//	file.Set(rk, rv);
//
//	W(k, rk, "hol");
//	W(v, rv, "budapest");
//	file.Set(rk, rv);
//
//	W(k, rk, "ki");
//	W(v, rv, "atka");
//	file.Set(rk, rv);
//
//	W(k, rk, "hol");
//	W(v, rv, "budapest");
//	file.Set(rk, rv);
//
//	W(k, rk, "ki");
//	file.Get(rk, rv);
//	P();
//
//	W(k, rk, "hol");
//	file.Get(rk, rv);
//	P();

	num = 50*1000;
	ksize = 20;
	vsize = 128;
	area = (char*) malloc(num*(ksize+vsize));

	catalog.Open("users");

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
//	}
//	elapsed = sw.Stop();
//	printf("%u sets took %ld msec\n", num, elapsed);
//	printf("Time spent in DataPage sw1: %ld msec\n", DataPage::sw1.Elapsed());

	sw.Start();
	for (int i = 0; i < 10*1000; i++)
	{
		k.Writef("%d", i);
		rk.Wrap(k);
		if (catalog.Get(rk, rv))
			PRINT()
		else
			ASSERT_FAIL();
	}	
	elapsed = sw.Stop();
	printf("%u gets took %ld msec\n", num, elapsed);
	
	catalog.Close();
							
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
