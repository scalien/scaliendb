#include "ClusterConnection.h"
#include "ClusterTransport.h"

void ClusterConnection::InitConnected(bool startRead)
{
	MessageConnection::InitConnected(startRead);
	
	Log_Trace();

	progress = INCOMING;
	nodeID = 0;
}

void ClusterConnection::SetTransport(ClusterTransport* transport_)
{
	Log_Trace();
	
	transport = transport_;
}

void ClusterConnection::SetNodeID(uint64_t nodeID_)
{
	nodeID = nodeID_;
}

void ClusterConnection::SetEndpoint(Endpoint& endpoint_)
{
	endpoint = endpoint_;
}

uint64_t ClusterConnection::GetNodeID()
{
	return nodeID;
}

ClusterConnection::Progress ClusterConnection::GetProgress()
{
	return progress;
}

void ClusterConnection::Connect()
{
	MessageConnection::Connect(endpoint);
}

void ClusterConnection::OnConnect()
{
	Buffer		buffer;
	ReadBuffer	rb;

	MessageConnection::OnConnect();
	
	Log_Trace("endpoint = %s", endpoint.ToString());
	
	AsyncRead();	
	
	rb = transport->GetEndpoint().ToReadBuffer();
	// send my nodeID:endpoint
	buffer.Writef("%U:%#R", transport->GetNodeID(), &rb);
	WritePriority(buffer);

	Log_Trace("Conn READY to node %" PRIu64 " at %s", nodeID, endpoint.ToString());

	progress = READY;
}

void ClusterConnection::OnClose()
{
	Log_Trace();
	
	if (connectTimeout.IsActive())
		return;
	
	Close();
	
	if (progress == INCOMING)
	{
		// we don't know the other side, delete conn
		transport->DeleteConnection(this);
	}
	else if (progress == READY)
	{
		// endpoint contains the other side, connect
		progress = OUTGOING;
		MessageConnection::Connect(endpoint);
	}
}

bool ClusterConnection::OnMessage(ReadBuffer& msg)
{
	uint64_t			nodeID;
	ReadBuffer			buffer;
	ClusterConnection*	dup;

	if (progress == ClusterConnection::INCOMING)
	{
		msg.Readf("%U:%#R", &nodeID, &buffer);
		dup = transport->GetConnection(nodeID);
		if (dup && nodeID != this->nodeID)
		{
			if (dup->progress == READY)
			{
				Log_Trace("delete conn");
				transport->DeleteConnection(this);		// drop current node
				return true;
			}
			else
			{
				Log_Trace("delete dup");
				transport->DeleteConnection(dup);		// drop dup
			}
		}
		progress = ClusterConnection::READY;
		this->nodeID = nodeID;
		this->endpoint.Set(buffer);
		Log_Trace("Conn READY to node %" PRIu64 " at %s", this->nodeID, this->endpoint.ToString());
		transport->AddConnection(this);
		transport->OnIncomingConnectionReady(this->nodeID, this->endpoint);
	}
	else if (progress == ClusterConnection::OUTGOING)
		ASSERT_FAIL();
	else
		transport->OnMessage(msg); // pass msg to upper layer
	
	return false;
}
