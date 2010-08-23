#include "ReplicationTransport.h"
#include "ReplicationManager.h"
#include "Quorums/QuorumContext.h"
#include "System/Config.h"

void ReplicationTransport::RegisterConnectionEvents(uint64_t contextID)
{
	registered.Append(contextID);
}

void ReplicationTransport::OnIncomingConnectionReady(uint64_t nodeID, Endpoint endpoint)
{
	uint64_t* it;
	
	for (it = registered.Head(); it != NULL; it = registered.Next(it))
		RMAN->GetContext(*it)->OnIncomingConnectionReady(nodeID, endpoint);
}

void ReplicationTransport::OnMessage(ReadBuffer msg)
{
	int			nread;
	uint64_t	contextID;
	
	Log_Trace("%.*s", P(&msg));
	
	nread = msg.Readf("%U:", &contextID);
	if (nread < 2)
		ASSERT_FAIL();
	
	msg.Advance(nread);
	
	RMAN->GetContext(contextID)->OnMessage(msg);
}
