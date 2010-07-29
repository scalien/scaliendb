#include "Controller.h"
#include "Framework/Replication/ReplicationManager.h"

bool Controller::HandleRequest(HttpConn* conn, const HttpRequest& request)
{
	Buffer buffer;
	
	if (context->IsLeaderKnown())
	{
		buffer.Writef("Master: %U\nSelf: %U\nPaxosID: %U\n", 
		 context->GetLeader(),
		 RMAN->GetNodeID(),
		 context->GetPaxosID());
	}
	else
	{
		buffer.Writef("No master, PaxosID: %U\n", context->GetPaxosID());
	}

	conn->Write(buffer.GetBuffer(), buffer.GetLength());
	conn->Flush(true);
	
	return true;
}

void Controller::SetContext(ControlConfigContext* context_)
{
	context = context_;
}
