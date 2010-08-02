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
	
	logID = readBuffer.GetCharAt(0) - '0'; // TODO: hack
	readBuffer.Advance(2);
	
	RMAN->GetContext(logID)->OnMessage();
}
