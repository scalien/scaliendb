#include "SDBPClient.h"
#include "SDBPClientConnection.h"
#include "Application/Common/ClientRequest.h"
#include "Application/Common/ClientResponse.h"

SDBPClient::SDBPClient()
{
	master = -1;
	commandID = 0;
	masterCommandID = 0;
	globalTimeout.SetCallable(MFUNC(SDBPClient, OnGlobalTimeout));
	masterTimeout.SetCallable(MFUNC(SDBPClient, OnMasterTimeout));
}

SDBPClient::~SDBPClient()
{
}

uint64_t SDBPClient::NextCommandID()
{
	return ++commandID * 10 + SDBP_MOD_COMMAND;
}

uint64_t SDBPClient::NextMasterCommandID()
{
	return ++masterCommandID * 10 + SDBP_MOD_GETMASTER;
}

ClientRequest* SDBPClient::CreateGetMasterRequest()
{
	ClientRequest*	req;
	
	req = new ClientRequest;
	req->GetMaster(NextMasterCommandID());
	
	return req;
}

void SDBPClient::ResendRequests(uint64_t nodeID)
{
	// for each request {
	//	if (request is sent on nodeID)
	//		send again
	// }
}

void SDBPClient::SetMaster(int64_t master_, uint64_t nodeID)
{
	Log_Trace("known master: %d, set master: %d, nodeID: %d", master, master_, nodeID);
	
	if (master_ == (int64_t) nodeID)
	{
		if (master != master_)
		{
			// node became the master
			Log_Message("Node %d is the master", nodeID);
			master = master_;

			// TODO: connectivity status
			//connectivityStatus = SUCCESS;
			
			// TODO: it is similar to ResendRequests
			//SendRequest(nodeID, safeCommands);
		}
		// else node is still the master
		EventLoop::Reset(&masterTimeout);
	}
	else if (master_ < 0 && master == (int64_t) nodeID)
	{
		// node lost its mastership
		Log_Message("Node %d lost its mastership", nodeID);
		master = -1;
		
		// TODO: connectivity status
		//connectivityStatus = KEYSPACE_NOMASTER;
		
		if (!IsSafe())
			return;

		// TODO: send safe requests that had no response to the new master
		// ResendRequests(nodeID);

		// TODO: What's this? -> set master timeout (copy-paste from Keyspace)
	}
}

void SDBPClient::UpdateConnectivityStatus()
{
	// TODO: check all connection's connect status
	// if there aren't any connected nodes, set the
	// connectivityStatus to NOCONNECTION
}

void SDBPClient::OnGlobalTimeout()
{
	// timeoutStatus = SDBP_GLOBAL_TIMEOUT;
}

void SDBPClient::OnMasterTimeout()
{
	// timeoutStatus = SDBP_MASTER_TIMEOUT;
}

bool SDBPClient::IsSafe()
{
	// TODO:
	return true;
}
