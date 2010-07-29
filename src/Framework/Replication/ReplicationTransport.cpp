#include "ReplicationTransport.h"
#include "ReplicationManager.h"
#include "Quorums/QuorumContext.h"
#include "System/Config.h"

ReadBuffer ReplicationTransport::GetMessage()
{
	return readBuffer;
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
