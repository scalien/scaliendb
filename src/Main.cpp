#include "System/Config.h"
#include "Framework/Database/Database.h"
#include "Framework/Replication/ReplicationManager.h"
#include "Application/Controller/ControlConfigContext.h"
#include "Application/Controller/Controller.h"
#include "Application/DataNode/DataNode.h"
#include "Application/HTTP/HttpServer.h"

int main(int argc, char** argv)
{
	DatabaseConfig				dbConfig;
	Database					db;
	ControlConfigContext		ctx;
	Controller					controller;
	DataNode					dataNode;
	SingleQuorum				quorum;
	QuorumDatabase				qdb;
	QuorumTransport				qtransport;
	Buffer						prefix;
	HttpServer					httpServer;
	Endpoint					endpoint;
	uint64_t					nodeID;
	bool						isController;
	const char*					role;
	
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
	}
	
	const char* s = configFile.GetListValue("controllers", RMAN->GetNodeID(), NULL);
	endpoint.Set(s);
	RMAN->GetTransport()->Init(RMAN->GetNodeID(), endpoint);
	
	unsigned numNodes = configFile.GetListNum("controllers");
	for (unsigned i = 0; i < numNodes; i++)
	{
		const char* s = configFile.GetListValue("controllers", i, NULL);
		endpoint.Set(s);
		quorum.AddNode(i);

		// TODO: i will not be a nodeID once we start using UIDs
		RMAN->GetTransport()->AddEndpoint(i, endpoint);
	}

	if (isController)
	{
		qdb.Init(db.GetTable("keyspace"));
		prefix.Write("0");
		qtransport.SetPriority(true);
		qtransport.SetPrefix(prefix);
		qtransport.SetQuorum(&quorum);
		
		ctx.SetContextID(0);
		ctx.SetController(&controller);
		ctx.SetQuorum(quorum);
		ctx.SetDatabase(qdb);
		ctx.SetTransport(qtransport);
		ctx.Start();

		controller.Init(db.GetTable("keyspace"));
		controller.SetConfigContext(&ctx);
		RMAN->GetTransport()->SetController(&controller); // TODO: hack
		RMAN->AddContext(&ctx);

		httpServer.Init(configFile.GetIntValue("http.port", 8080));
		httpServer.RegisterHandler(&controller);
	}
	else
	{
		dataNode.Init(db.GetTable("keyspace"));
		httpServer.Init(configFile.GetIntValue("http.port", 8080));
		httpServer.RegisterHandler(&dataNode);
	}
	
	
	EventLoop::Init();
	EventLoop::Run();
	EventLoop::Shutdown();
}
