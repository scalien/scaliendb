#ifndef REPLICATIONTRANSPORT_H
#define REPLICATIONTRANSPORT_H

#include "System/Containers/InList.h"
#include "Framework/Messaging/MessageTransport.h"

class Message;		// forward
class Controller;	// forward
/*
===============================================================================

 ReplicationTransport

===============================================================================
*/

class ReplicationTransport : public MessageTransport
{
public:
	ReplicationTransport();
	
	ReadBuffer		GetMessage();
	void			SetController(Controller* controller);

private:
	virtual void	OnIncomingConnectionReady(uint64_t nodeID, Endpoint endpoint);
	virtual void	OnMessage(ReadBuffer msg);

	ReadBuffer		readBuffer;
	Controller*		controller; // TODO: huge hack
};

#endif
