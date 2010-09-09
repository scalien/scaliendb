#include "ContextTransport.h"
#include "ReplicationConfig.h"
#include "Quorums/QuorumContext.h"
#include "System/Config.h"

static uint64_t Hash(uint64_t ID)
{
	return ID;
}

ContextTransport* replicationTransport = NULL;

ContextTransport* ContextTransport::Get()
{
	if (replicationTransport == NULL)
		replicationTransport = new ContextTransport();
	
	return replicationTransport;
}

ContextTransport::ContextTransport()
{
	clusterContext = NULL;
}

void ContextTransport::SetClusterContext(ClusterContext* context)
{
	clusterContext = context;
}

ClusterContext* ContextTransport::GetClusterContext()
{
	return clusterContext;
}

void ContextTransport::AddQuorumContext(QuorumContext* context)
{
	uint64_t contextID = context->GetContextID();
	contextMap.Set(contextID, context);
}

QuorumContext* ContextTransport::GetQuorumContext(uint64_t contextID)
{
	QuorumContext* pcontext;
	
	assert(contextMap.HasKey(contextID));
	pcontext = NULL;
	contextMap.Get(contextID, pcontext);
	
	return pcontext;
}

void ContextTransport::OnIncomingConnectionReady(uint64_t nodeID, Endpoint endpoint)
{
	if (clusterContext)
		clusterContext->OnIncomingConnectionReady(nodeID, endpoint);
}

void ContextTransport::OnMessage(ReadBuffer msg)
{
	char proto;
	
	Log_Trace("%.*s", P(&msg));
	
	if (msg.GetLength() < 2)
		ASSERT_FAIL();

	proto = msg.GetCharAt(0);
	assert(msg.GetCharAt(1) == ':');

	msg.Advance(2);
	
	switch (proto)
	{
		case PROTOCOL_CLUSTER:
			OnClusterMessage(msg);
			break;
		case PROTOCOL_QUORUM:
			OnQuorumMessage(msg);
			break;
		default:
			ASSERT_FAIL();
			break;
	}
}

void ContextTransport::OnClusterMessage(ReadBuffer& msg)
{
	if (clusterContext)
		clusterContext->OnClusterMessage(msg);
}

void ContextTransport::OnQuorumMessage(ReadBuffer& msg)
{
	int			nread;
	uint64_t	contextID;

	nread = msg.Readf("%U:", &contextID);
	if (nread < 2)
		ASSERT_FAIL();
	
	msg.Advance(nread);

	GetQuorumContext(contextID)->OnMessage(msg);
}
