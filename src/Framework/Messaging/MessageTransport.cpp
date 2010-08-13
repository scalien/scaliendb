#include "MessageTransport.h"

void MessageTransport::Init(uint64_t nodeID, Endpoint& endpoint)
{
	selfNodeID = nodeID;
	selfEndpoint = endpoint;
	server.Init(endpoint.GetPort());
	server.SetTransport(this);
}

void MessageTransport::OnIncomingConnection(MessageConnection* conn)
{
	Node*	node;
	
	Log_Trace();
	
	node = new Node;
	node->state = Node::INCOMING;
	node->nodeID = 0;
	node->conn = conn;
	conn->SetNode(node);
}

void MessageTransport::OnOutgoingConnection(MessageConnection* conn)
{
	Buffer		buffer;
	ReadBuffer	rb;
	Node*		node;
	
	node = conn->GetNode();
	
	rb = node->endpoint.ToReadBuffer();
	// send my nodeID:endpoint
	buffer.Writef("%U:%#R", selfNodeID, &rb);
	conn->WritePriority(buffer);
	Log_Trace("Conn READY to node %" PRIu64 " at %s", node->nodeID, node->endpoint.ToString());
	node->state = Node::READY;
}

void MessageTransport::OnClose(MessageConnection* conn)
{
	Node*		node;
	
	node = conn->GetNode();
	assert(node != NULL);
	
	if (node->state == Node::INCOMING)
	{
		// we don't know the other side, delete conn
		DeleteNode(node);
	}
	else if (node->state == Node::READY)
	{
		// node->endpoint contains the other side, connect
		node->state = Node::OUTGOING;
		node->conn->Connect(node->endpoint);
	}
}

void MessageTransport::OnRead(MessageConnection* conn, ReadBuffer msg)
{
	Node*		node;
	Node*		dup;
	uint64_t	nodeID;
	ReadBuffer	buffer;
	
	Log_Trace();
	
	node = conn->GetNode();

	if (node->state == Node::INCOMING)
	{
		msg.Readf("%U:%#R", &nodeID, &buffer);
		dup = GetNode(nodeID);
		if (dup && nodeID != selfNodeID)
		{
			if (dup->state == Node::READY)
			{
				Log_Trace("delete node");
				DeleteNode(node);				// drop current node
				return;
			}
			else
			{
				Log_Trace("delete dup");
				DeleteNode(dup);				// drop dup
			}
		}
		node->state = Node::READY;
		node->nodeID = nodeID;
		node->endpoint.Set(buffer);
		Log_Trace("Conn READY to node %" PRIu64 " at %s", node->nodeID, node->endpoint.ToString());
		nodes.Append(node);
		OnIncomingConnectionReady(node->nodeID, node->endpoint);
	}
	else if (node->state == Node::OUTGOING)
	{
		ASSERT_FAIL();
	}
	else
	{
		// pass msg to upper layer
		OnMessage(msg);
	}
}

void MessageTransport::AddEndpoint(uint64_t nodeID, Endpoint endpoint)
{
	Node* node;
	
	if (selfNodeID < nodeID)
		return;
	
	node = GetNode(nodeID);
	
	if (node != NULL)
		return;
	
	if (nodeID == selfNodeID)
		Log_Trace("connecting to self");
	
	node = new Node;
	node->state = Node::OUTGOING;
	node->nodeID = nodeID;
	node->endpoint = endpoint;

	node->conn = new MessageConnection;
	node->conn->SetTransport(this);
	node->conn->SetNode(node);
	node->conn->Connect(node->endpoint);

	nodes.Append(node);	
}

unsigned MessageTransport::GetNumEndpoints()
{
	return nodes.GetLength();
}

void MessageTransport::SendMessage(uint64_t nodeID, Buffer& prefix, Message& msg)
{
	Node*	it;
	
	for (it = nodes.Head(); it != NULL; it = nodes.Next(it))
	{
		if (it->nodeID == nodeID)
		{
			if (it->state != Node::READY)
			{
				Log_Trace("connection to %" PRIu64 " has state: %d", nodeID, it->state);
				return;
			}
			it->conn->Write(prefix, msg);
			return;
		}
	}	
	Log_Trace("no connection to nodeID %" PRIu64, nodeID);
}

void MessageTransport::SendPriorityMessage(uint64_t nodeID, Buffer& prefix, Message& msg)
{
	Node*	it;

	for (it = nodes.Head(); it != NULL; it = nodes.Next(it))
	{
		if (it->nodeID == nodeID)
		{
			if (it->state != Node::READY)
			{
				Log_Trace("connection to %" PRIu64 " has state: %d", nodeID, it->state);
				return;
			}
			it->conn->WritePriority(prefix, msg);
			return;
		}
	}
	Log_Trace("no connection to nodeID %" PRIu64, nodeID);
}

Node* MessageTransport::GetNode(uint64_t nodeID)
{
	Node*	it;
	
	for (it = nodes.Head(); it != NULL; it = nodes.Next(it))
	{
		if (it->nodeID == nodeID)
			return it;
	}
	
	return NULL;
}

void MessageTransport::DeleteNode(Node* node)
{
	node->conn->Close();
	delete node->conn; // TODO: what happens when control returns to OnRead()
	if (node->next != node) // TODO:
		nodes.Remove(node);
	delete node;
}
