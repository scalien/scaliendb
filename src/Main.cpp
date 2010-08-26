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

#include <map>
#include <stdlib.h>

static inline int KeyCmp(const ReadBuffer& a, const ReadBuffer& b)
{
	return ReadBuffer::Cmp(a, b);
}

bool operator< (const ReadBuffer& left, const ReadBuffer& right)
{
	return ReadBuffer::LessThan(left, right);
}

static ReadBuffer Key(KeyValue* kv)
{
	return kv->key;
}

void GenRandomString(ReadBuffer& bs, size_t length)
{
	const char set[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
	const size_t setsize = sizeof(set) - 1;
	unsigned int i;
	static uint64_t d = Now();
	char* buffer;

	assert(bs.GetLength() >= length);
	buffer = bs.GetBuffer();
	
	for (i = 0; i < length; i++) {
			// more about why these numbers were chosen:
			// http://en.wikipedia.org/wiki/Linear_congruential_generator
			d = (d * 1103515245UL + 12345UL) >> 2;
			buffer[i] = set[d % setsize];
	}

	bs.SetLength(length);
}

int TestTreeMap()
{
	InTreeMap<KeyValue>						kvs;
	ReadBuffer								rb;
	ReadBuffer								rk;
	ReadBuffer								rv;
	Buffer									buf;
	KeyValue*								kv;
	KeyValue*								it;
	Stopwatch								sw;
	const unsigned							num = 1000000;
	unsigned								ksize;
	unsigned								vsize;
	char*									area;
	char*									kvarea;
	char*									p;
	int										len;
	int										cmpres;
	std::map<ReadBuffer, ReadBuffer>		bufmap;

	ksize = 20;
	vsize = 128;
	area = (char*) malloc(num*(ksize+vsize));
	kvarea = (char*) malloc(num * sizeof(KeyValue));
	
	//sw.Start();
	for (unsigned u = 0; u < num; u++)
	{
		p = area + u*(ksize+vsize);
		len = snprintf(p, ksize, "%u", u);
		rk.SetBuffer(p);
		rk.SetLength(len);
		p += ksize;
		len = snprintf(p, vsize, "%.100f", (float) u);
		rv.SetBuffer(p);
		rv.SetLength(len);

//		buf.Writef("%u", u);
//		rb.Wrap(buf);
//		kv = new KeyValue;
//		kv->SetKey(rb, true);
//		kv->SetValue(rb, true);

		kv = (KeyValue*) (kvarea + u * sizeof(KeyValue));
		kv->SetKey(rk, false);
		kv->SetValue(rv, false);
		
		sw.Start();
		kvs.Insert(kv);
		//bufmap.insert(std::pair<ReadBuffer, ReadBuffer>(rk, rv));
		sw.Stop();
	}
	printf("insert time: %ld\n", sw.Elapsed());

	//return 0;

	sw.Reset();
	sw.Start();
	for (unsigned u = 0; u < num; u++)
	{
		buf.Writef("%u", u);
		rb.Wrap(buf);
		kv = kvs.Get<ReadBuffer&>(rb);
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
		kv = kvs.Delete<ReadBuffer&>(rb);
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
		kv = kvs.Get<ReadBuffer&>(rb);
		if (kv != NULL)
			ASSERT_FAIL();
	}
	sw.Stop();
	printf("get time: %ld\n", sw.Elapsed());

	sw.Reset();
	for (unsigned u = 0; u < num; u++)
	{
		p = area + u*(ksize+vsize);
		rk.SetBuffer(p);
		rk.SetLength(ksize);
		GenRandomString(rk, ksize);
		p += ksize;
		rv.SetBuffer(p);
		rv.SetLength(vsize);
		GenRandomString(rv, vsize);

		sw.Start();
		it = kvs.Locate<ReadBuffer&>(rk, cmpres);
		if (cmpres == 0 && it != NULL)
		{
			// found, overwrite value
			it->value = rv;
		}
		else
		{
			kv = (KeyValue*) (kvarea + u * sizeof(KeyValue));
			kv->SetKey(rk, false);
			kv->SetValue(rv, false);
			kvs.InsertAt(kv, it, cmpres);
		}
		sw.Stop();
	}
	printf("random insert time: %ld\n", sw.Elapsed());

	unsigned u = 0;
	sw.Reset();
	sw.Start();
	for (it = kvs.First(); it != NULL; it = kvs.Next(it))
	{
		//printf("it->key = %.*s\n", P(&it->key));
		//kv = it; // dummy op
		u++;
	}
	sw.Stop();
	printf("found: %u\n", u);
	printf("iteration time: %ld\n", sw.Elapsed());

	return 0;
}

int TestClock()
{
	Stopwatch	sw;
	uint64_t	now;
	uint64_t	tmp;
	unsigned	num;
	
	num = 10 * 1000 * 1000;
	
	sw.Reset();
	sw.Start();
	for (unsigned u = 0; u < num; u++)
		now = NowClock();
	sw.Stop();
	printf("Elapsed without clock: %ld msec\n", sw.Elapsed());
	
	StartClock();
	MSleep(100);
	
	now = NowClock();
	sw.Reset();
	sw.Start();
	for (unsigned u = 0; u < num; u++)
	{
		tmp = NowClock();
		if (tmp != now)
		{
			printf("Clock changed from %" PRIu64 " to %" PRIu64 "\n", now, tmp);
			now = tmp;
		}
	}
	sw.Stop();
	printf("Elapsed with clock: %ld msec\n", sw.Elapsed());
	
	StopClock();
	
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
//	return TestClock();
	
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

	num = 100*1000;
	ksize = 20;
	vsize = 128;
	area = (char*) malloc(num*(ksize+vsize));

	catalog.Open("dogs");

	sw.Start();
	for (int i = 0; i < num; i++)
	{
		p = area + i*(ksize+vsize);
		len = snprintf(p, ksize, "%d", i);
		rk.SetBuffer(p);
		rk.SetLength(len);
		p += ksize;
		len = snprintf(p, vsize, "%.100f", (float) i);
		rv.SetBuffer(p);
		rv.SetLength(len);
		catalog.Set(rk, rv, false);
	}
	elapsed = sw.Stop();
	printf("%u sets took %ld msec\n", num, elapsed);
	printf("Time spent in DataPage sw1: %ld msec\n", DataPage::sw1.Elapsed());

	sw.Start();
	for (int i = 0; i < num; i++)
	{
		k.Writef("%d", i);
		rk.Wrap(k);
		if (catalog.Get(rk, rv))
			;//PRINT()
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
