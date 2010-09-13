#include "DataNode.h"
#include "System/Common.h"
#include "System/Platform.h"
#include "Framework/Replication/ReplicationConfig.h"
#include "Application/HTTP/HTTPConsts.h"

void DataNode::Init(Table* table_)
{
	table = table_;
	
	if (!table->Get(NULL, "#nodeID", nodeID))
	{
		nodeID = RandomInt(0, 64000); //gen_uuid();
		Log_Trace("%" PRIu64, nodeID);
		table->Set(NULL, "#nodeID", nodeID);
	}
	REPLICATION_CONFIG->SetNodeID(nodeID);
	httpHandler.Init(this);
	Log_Trace("nodeID = %" PRIu64, nodeID);
}

DataService* DataNode::GetService()
{
	return &httpHandler;
}

HTTPHandler* DataNode::GetHandler()
{
	return &httpHandler;
}
