#ifndef SDBPCLIENTCONNECTION_H
#define SDBPCLIENTCONNECTION_H

#include "System/Containers/List.h"
#include "System/Platform.h"
#include "Framework/Messaging/MessageConnection.h"
#include "Application/Common/ClientResponse.h"
#include "SDBPClient.h"

namespace SDBPClient
{

/*
===============================================================================================

 SDBPClient::ControllerConnection

===============================================================================================
*/

class ControllerConnection : public MessageConnection
{
public:
	ControllerConnection(Client* client, uint64_t nodeID, Endpoint& endpoint);
	
	void			Connect();
	void			Send(ClientRequest* request);
	void			SendGetConfigState();

	uint64_t		GetNodeID();

	void			OnGetConfigStateTimeout();	

	// MessageConnection interface
	virtual bool	OnMessage(ReadBuffer& msg);
	virtual void	OnWrite();
	virtual void	OnConnect();
	virtual void	OnClose();
	
private:
	typedef InList<Request> RequestList;

	bool			ProcessResponse(ClientResponse* resp);
	bool			ProcessGetConfigState(ClientResponse* resp);
	bool			ProcessGetMaster(ClientResponse* resp);
	bool			ProcessCommandResponse(ClientResponse* resp);
	Request*		RemoveRequest(uint64_t commandID);

	Client*			client;
	uint64_t		nodeID;
	Endpoint		endpoint;
	uint64_t		getConfigStateTime;
	Countdown		getConfigStateTimeout;
	bool			getConfigStateSent;
	RequestList		requests;
};

}; // namespace

#endif
