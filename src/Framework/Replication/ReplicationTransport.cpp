#include "ReplicationTransport.h"
#include "ReplicationManager.h"
#include "Quorums/QuorumContext.h"
#include "System/Config.h"
#include "Application/Controller/Controller.h" // TODO

ReadBuffer ReplicationTransport::GetMessage()
{
	return readBuffer;
}

void ReplicationTransport::SetController(Controller* controller_)
{
	controller = controller_;
}

void ReplicationTransport::OnIncomingConnectionReady(uint64_t nodeID, Endpoint endpoint)
{
	if (controller)
		controller->OnIncomingConnectionReady(nodeID, endpoint);
}

void ReplicationTransport::OnMessage(ReadBuffer msg)
{
	uint64_t logID;
	
	Log_Trace();
	
	// TODO: parse
	
	readBuffer = msg;
	Log_Trace("%.*s", P(&readBuffer));
	readBuffer.Advance(2);
	
	logID = 0;
	
	RMAN->GetContext(logID)->OnMessage();
}
