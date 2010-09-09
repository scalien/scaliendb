#include "ClusterTransport.h"

void ClusterTransport::Init(uint64_t nodeID_, Endpoint& endpoint_)
{
	nodeID = nodeID_;
	endpoint = endpoint_;
	server.Init(endpoint.GetPort());
	server.SetTransport(this);
}

uint64_t ClusterTransport::GetNodeID()
{
	return nodeID;
}

Endpoint& ClusterTransport::GetEndpoint()
{
	return endpoint;
}

void ClusterTransport::AddEndpoint(uint64_t nodeID, Endpoint endpoint)
{
	ClusterConnection* conn;

	if (nodeID < this->nodeID)
		return;
	
	conn = GetConnection(nodeID);
	if (conn != NULL)
		return;

	if (nodeID == this->nodeID)
		Log_Trace("connecting to self");

	conn = new ClusterConnection;
	conn->SetTransport(this);
	conn->SetNodeID(nodeID);
	conn->SetEndpoint(endpoint);
	conn->Connect();	
}

void ClusterTransport::SendMessage(uint64_t nodeID, Buffer& prefix, Message& msg)
{
	ClusterConnection*	conn;
	
	conn = GetConnection(nodeID);
	
	if (!conn)
	{
		Log_Trace("no connection to nodeID %" PRIu64, nodeID);
		return;
	}
	
	if (conn->GetProgress() != ClusterConnection::READY)
	{
		Log_Trace("connection to %" PRIu64 " has progress: %d", nodeID, conn->GetProgress());
		return;
	}
	
	msg.Write(msgBuffer);
	conn->Write(prefix, msgBuffer);
}

void ClusterTransport::SendPriorityMessage(uint64_t nodeID, Buffer& prefix, Message& msg)
{
	ClusterConnection*	conn;
	
	conn = GetConnection(nodeID);
	
	if (!conn)
	{
		Log_Trace("no connection to nodeID %" PRIu64, nodeID);
		return;
	}
	
	if (conn->GetProgress() != ClusterConnection::READY)
	{
		Log_Trace("connection to %" PRIu64 " has progress: %d", nodeID, conn->GetProgress());
		return;
	}
	
	msg.Write(msgBuffer);
	conn->WritePriority(prefix, msgBuffer);
}

void ClusterTransport::AddConnection(ClusterConnection* conn)
{
	conns.Append(conn);
}

ClusterConnection* ClusterTransport::GetConnection(uint64_t nodeID)
{
	ClusterConnection* it;
	
	for (it = conns.First(); it != NULL; it = conns.Next(it))
	{
		if (it->GetNodeID() == nodeID)
			return it;
	}
	
	return NULL;
}

void ClusterTransport::DeleteConnection(ClusterConnection* conn)
{
	conn->Close();

	if (conn->next != conn)
		conns.Remove(conn);

	delete conn;
}
