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
	
	node = new Node;
	node->state = Node::INCOMING;
	node->nodeID = 0;
	node->conn = conn;
	conn->SetNode(node);
}

void MessageTransport::OnOutgoingConnection(MessageConnection* conn)
{
	Buffer		buffer;
	Buffer		epBuffer;
	ReadBuffer	rb;
	Node*		node;
	
	node = conn->GetNode();
	
	// TODO: get rid of epBuffers!!!!
	epBuffer.Write(selfEndpoint.ToString()); 
	rb.Wrap(epBuffer);
	// send my nodeID:endpoint
	buffer.Writef("%U:%M", selfNodeID, &rb);
	conn->WritePriority(buffer);
	
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
	Buffer		epBuffer;
	
	node = conn->GetNode();

	if (node->state == Node::INCOMING)
	{
		msg.Readf("%U:%M", &nodeID, &buffer);
		dup = GetNode(nodeID);
		if (dup)
		{
			if (dup->state == Node::READY)
			{
				DeleteNode(node);				// drop current node
				return;
			}
			else
				DeleteNode(dup);				// drop dup
		}
		node->state = Node::READY;
		node->nodeID = nodeID;
		// HACK TODO
		epBuffer.Write(buffer);
		epBuffer.NullTerminate();
		node->endpoint.Set(epBuffer.GetBuffer());
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
	
	if (nodeID == selfNodeID)
		return;
	
	node = GetNode(nodeID);
	
	if (node != NULL)
		return;
	
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

void MessageTransport::SendMessage(uint64_t nodeID,
 const Buffer& prefix, const Message& msg)
{
	Node*	it;
	
	if (nodeID == selfNodeID)
	{
		// TODO: huge hack
		unsigned len;
		Buffer	head;
		Buffer	buffer;
		ReadBuffer rb;
		
		if (!msg.Write(buffer))
			ASSERT_FAIL();
		len = prefix.GetLength() + 1 + buffer.GetLength();
		head.Writef("%B:", &prefix);
		
		Log_Trace("%.*s%.*s", P(&head), P(&buffer));
		
		head.Append(buffer);
		rb.Wrap(head);
		OnMessage(rb);		
	}
	
	for (it = nodes.Head(); it != NULL; it = nodes.Next(it))
	{
		if (it->nodeID == nodeID)
		{
			if (it->state != Node::READY)
				return;				
			it->conn->Write(prefix, msg);
			break;
		}
	}
}

void MessageTransport::SendPriorityMessage(uint64_t nodeID,
 const Buffer& prefix, const Message& msg)
{
	Node*	it;

	if (nodeID == selfNodeID)
	{
		// TODO: huge hack
		unsigned len;
		Buffer	head;
		Buffer	buffer;
		ReadBuffer rb;
		
		if (!msg.Write(buffer))
			ASSERT_FAIL();
		len = prefix.GetLength() + 1 + buffer.GetLength();
		head.Writef("%B:", &prefix);
		
		Log_Trace("%.*s%.*s", P(&head), P(&buffer));
		
		head.Append(buffer);
		rb.Wrap(head);
		OnMessage(rb);		
	}

	for (it = nodes.Head(); it != NULL; it = nodes.Next(it))
	{
		if (it->nodeID == nodeID)
		{
			if (it->state != Node::READY)
				return;
			it->conn->WritePriority(prefix, msg);
			break;
		}
	}
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
