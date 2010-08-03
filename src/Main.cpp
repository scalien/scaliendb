#include "System/Config.h"
#include "Framework/Database/Database.h"
#include "Framework/Replication/ReplicationManager.h"
#include "Application/Controller/ControlConfigContext.h"
#include "Application/Controller/Controller.h"
#include "Application/DataNode/DataNode.h"
#include "Application/DataNode/DataChunkContext.h"
#include "Application/HTTP/HttpServer.h"

int main(int argc, char** argv)
{
	DatabaseConfig				dbConfig;
	Database					db;
	ControlConfigContext		configContext;
	DataChunkContext			dataContext;
	Controller					controller;
	DataNode					dataNode;
	SingleQuorum				singleQuorum;
	QuorumDatabase				qdb;
	QuorumTransport				configTransport;
	QuorumTransport				chunkTransport;
	Buffer						prefix;
	HttpServer					httpServer;
	Endpoint					endpoint;
	uint64_t					nodeID;
	bool						isController;
	const char*					role;
	
	randseed();
	configFile.Init(argv[1]);
	role = configFile.GetValue("role", "data");
	if (strcmp(role, "data") == 0)
		isController = false;
	else
		isController = true;

	Log_SetTarget(LOG_TARGET_STDOUT);
	Log_SetTrace(true);
	Log_SetTimestamping(true);

	IOProcessor::Init(1024);

	dbConfig.dir = configFile.GetValue("database.dir", DATABASE_CONFIG_DIR);
	db.Init(dbConfig);

	if (isController)
	{
		nodeID = configFile.GetIntValue("nodeID", 0);
		RMAN->SetNodeID(nodeID);
		const char* s = configFile.GetListValue("controllers", RMAN->GetNodeID(), NULL);
		endpoint.Set(s);
		RMAN->GetTransport()->Init(RMAN->GetNodeID(), endpoint);
	}
	else
	{
		dataNode.Init(db.GetTable("chunk1")); // sets RMAN->nodeID
		const char* s = configFile.GetValue("endpoint", NULL);
		endpoint.Set(s);
		RMAN->GetTransport()->Init(RMAN->GetNodeID(), endpoint);
	}

	unsigned numNodes = configFile.GetListNum("controllers");
	for (unsigned i = 0; i < numNodes; i++)
	{
		const char* s = configFile.GetListValue("controllers", i, NULL);
		endpoint.Set(s);

		// TODO: i will not be a nodeID once we start using UIDs
		RMAN->GetTransport()->AddEndpoint(i, endpoint);

		if (isController)
			singleQuorum.AddNode(i);
	}

	if (isController)
	{
		qdb.Init(db.GetTable("chunk0"));
		prefix.Write("0");
		configTransport.SetPriority(true);
		configTransport.SetPrefix(prefix);
		configTransport.SetQuorum(&singleQuorum);
		
		configContext.SetContextID(0);
		configContext.SetController(&controller);
		configContext.SetQuorum(singleQuorum);
		configContext.SetDatabase(qdb);
		configContext.SetTransport(configTransport);
		configContext.Start();

		controller.Init(db.GetTable("chunk0"));
		RMAN->GetTransport()->SetController(&controller); // TODO: hack
		RMAN->AddContext(&configContext);

		httpServer.Init(configFile.GetIntValue("http.port", 8080));
		httpServer.RegisterHandler(&controller);
	}
	else
	{
		qdb.Init(db.GetTable("chunk1"));
		prefix.Write("1");
		chunkTransport.SetPriority(true);
		chunkTransport.SetPrefix(prefix);
		chunkTransport.SetQuorum(dataContext.GetQuorum());	

		dataContext.SetContextID(1);
		dataContext.SetDatabase(qdb); // TODO: hack
		dataContext.SetTransport(chunkTransport);
		dataContext.Start();

		httpServer.Init(configFile.GetIntValue("http.port", 8080));
		httpServer.RegisterHandler(&dataNode);
		RMAN->AddContext(&dataContext);
	}
	
	EventLoop::Init();
	EventLoop::Run();
	EventLoop::Shutdown();
}
