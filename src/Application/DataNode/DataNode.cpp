#include "DataNode.h"
#include "System/Common.h"
#include "System/Platform.h"
#include "Framework/Replication/ReplicationManager.h"

void DataNode::Init(Table* table_)
{
	table = table_;
	
	if (!table->Get(NULL, "nodeID", nodeID))
	{
		nodeID = gen_uuid();
		Log_Trace("%" PRIu64, nodeID);
		table->Set(NULL, "nodeID", nodeID);
	}
	RMAN->SetNodeID(nodeID);
	Log_Trace("nodeID = %" PRIu64, nodeID);
}

bool DataNode::HandleRequest(HttpConn* conn, const HttpRequest& request)
{
	Buffer buffer;

	buffer.Writef("nodeID: %U", nodeID);

	conn->Write(buffer.GetBuffer(), buffer.GetLength());
	conn->Flush(true);
	
	return true;
}

