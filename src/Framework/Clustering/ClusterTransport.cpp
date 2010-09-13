#include "ClusterTransport.h"

void ClusterTransport::Init(Endpoint& endpoint_)
{
	endpoint = endpoint_;
	server.Init(endpoint.GetPort());
	server.SetTransport(this);
	awaitingNodeID = true;
}

void ClusterTransport::SetSelfNodeID(uint64_t nodeID_)
{
	nodeID = nodeID_;
	awaitingNodeID = false;
	ReconnectAll(); // so we establish "regular" connections with the nodeID
}

bool ClusterTransport::IsAwaitingNodeID()
{
	return awaitingNodeID;
}

uint64_t ClusterTransport::GetSelfNodeID()
{
	return nodeID;
}

Endpoint& ClusterTransport::GetSelfEndpoint()
{
	return endpoint;
}

void ClusterTransport::AddNode(uint64_t nodeID, Endpoint& endpoint)
{
	ClusterConnection* conn;

//	if (nodeID < this->nodeID)
//		return;
	
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

bool ClusterTransport::SetConnectionNodeID(Endpoint& endpoint, uint64_t nodeID)
{
	ClusterConnection* it;
	
	for (it = conns.First(); it != NULL; it = conns.Next(it))
	{
		if (it->GetEndpoint() == endpoint)
		{
			assert(it->GetProgress() == ClusterConnection::AWAITING_NODEID);
			it->SetNodeID(nodeID);
			it->SetProgress(ClusterConnection::READY);
			return true;
		}
	}
	
	return false;
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

void ClusterTransport::DropConnection(uint64_t nodeID)
{
	ClusterConnection* conn;
	
	conn = GetConnection(nodeID);
	
	if (!conn)
		return;
		
	DeleteConnection(conn);
}

void ClusterTransport::DropConnection(Endpoint endpoint)
{
	ClusterConnection* conn;
	
	conn = GetConnection(endpoint);
	
	if (!conn)
		return;
		
	DeleteConnection(conn);
}

bool ClusterTransport::GetNextWaiting(Endpoint& endpoint)
{
	ClusterConnection* it;
	
	for (it = conns.First(); it != NULL; it = conns.Next(it))
	{
		if (it->GetProgress() == ClusterConnection::AWAITING_NODEID)
		{
			endpoint = it->GetEndpoint();
			return true;
		}
	}
	
	return false;
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
		if (it->GetNodeID() == nodeID && it->GetProgress() != ClusterConnection::AWAITING_NODEID)
			return it;
	}
	
	return NULL;
}

ClusterConnection* ClusterTransport::GetConnection(Endpoint& endpoint)
{
	ClusterConnection* it;
	
	for (it = conns.First(); it != NULL; it = conns.Next(it))
	{
		if (it->GetEndpoint() == endpoint)
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

void ClusterTransport::ReconnectAll()
{
	ClusterConnection* it;
	
	for (it = conns.First(); it != NULL; it = conns.Next(it))
	{
		it->Close();
		it->Connect();
	}
}
