#include "ReplicationTransport.h"
#include "ReplicationManager.h"
#include "Quorums/QuorumContext.h"
#include "System/Config.h"

void ReplicationTransport::Start()
{
	Node*		it;
	Endpoint	endpoint;

	reader.SetOnRead(MFUNC(ReplicationTransport, OnRead));

	Log_Trace("%d", RMAN->GetNodeID());

	endpoint = nodes.Get(RMAN->GetNodeID()).endpoint;
	
	
	if (!reader.Init(endpoint.GetPort()))
		STOP_FAIL("cannot bind reader port", 1);

	// create writers for controllers only
	for (it = nodes.Head(); it != NULL; it = nodes.Next(it))
		it->writer.Init(it->endpoint);
}

void ReplicationTransport::AddEndpoint(uint64_t nodeID, Endpoint endpoint)
{
	Node* node;
	
	node = new Node;
	node->nodeID = nodeID;
	node->endpoint = endpoint;
	nodes.Append(node);
}

unsigned ReplicationTransport::GetNumEndpoints()
{
	return nodes.GetLength();
}

Endpoint ReplicationTransport::GetEndpoint(uint64_t nodeID)
{
	return nodes.Get(nodeID).endpoint;
}


void ReplicationTransport::Shutdown()
{
	Node*	it;
		
	reader.Close();
	
	for (it = nodes.Head(); it != NULL; it = nodes.Delete(it))
		it->writer.Close();
}

void ReplicationTransport::SendMessage(uint64_t nodeID,
 const Buffer& prefix, const Message& msg)
{
	Node*	it;

	for (it = nodes.Head(); it != NULL; it = nodes.Next(it))
	{
		if (it->nodeID == nodeID)
		{
			it->writer.Write(prefix, msg);
			break;
		}
	}
}

void ReplicationTransport::SendPriorityMessage(uint64_t nodeID,
 const Buffer& prefix, const Message& msg)
{
	Node*	it;

	for (it = nodes.Head(); it != NULL; it = nodes.Next(it))
	{
		if (it->nodeID == nodeID)
		{
			it->writer.WritePriority(prefix, msg);
			break;
		}
	}
}

ReadBuffer ReplicationTransport::GetMessage()
{
	return readBuffer;
}

void ReplicationTransport::OnRead()
{
	uint64_t logID;
	
	// TODO: parse
	
	logID = 0;
	
	RMAN->GetContext(logID)->OnMessage();
}
