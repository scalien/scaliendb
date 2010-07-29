#ifndef REPLICATIONTRANSPORT_H
#define REPLICATIONTRANSPORT_H

#include "System/Containers/InList.h"
#include "Framework/Messaging/MessageTransport.h"

class Message; // forward

/*
===============================================================================

 ReplicationTransport

===============================================================================
*/

class ReplicationTransport : public MessageTransport
{
public:
	ReadBuffer		GetMessage();

private:
	virtual void	OnMessage(ReadBuffer msg);

	ReadBuffer		readBuffer;
};

#endif
