#ifndef SDBPCLIENTCONNECTION_H
#define SDBPCLIENTCONNECTION_H

#include "Framework/Messaging/MessageConnection.h"
#include "System/Containers/List.h"
#include "System/Platform.h"

class SDBPClient;
class ClientRequest;
class ClientResponse;

#define SDBP_MOD_GETMASTER		0
#define SDBP_MOD_COMMAND		1

/*
===============================================================================================================

 SDBPClientConnection

===============================================================================
*/

class SDBPClientConnection : public MessageConnection
{
public:
	void			Init(SDBPClient* client, uint64_t nodeID, Endpoint& endpoint);
	void			Connect();
	void			Send(ClientRequest* request);
	void			SendGetMaster();

	void			OnReadTimeout();
	void			OnGetMasterTimeout();	

	// MessageConnection interface
	virtual bool	OnMessage(ReadBuffer& msg);
	virtual void	OnConnect();
	virtual void	OnConnectTimeout();
	virtual void	OnClose();
	
private:
	typedef List<ClientRequest*> RequestList;

	bool			ProcessResponse(ClientResponse* resp);
	bool			ProcessGetConfigState(ClientResponse* resp);
	bool			ProcessGetMaster(ClientResponse* resp);
	bool			ProcessCommandResponse(ClientResponse* resp);
	ClientRequest*	RemoveRequest(uint64_t commandID);

	SDBPClient*		client;
	uint64_t		nodeID;
	Endpoint		endpoint;
	uint64_t		getMasterTime;
	Countdown		getMasterTimeout;
	RequestList		requests;
};

#endif
