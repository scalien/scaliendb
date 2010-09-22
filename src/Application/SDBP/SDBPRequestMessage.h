#ifndef SDBPREQUESTMESSAGE_H
#define SDBPREQUESTMESSAGE_H

#include "Framework/Messaging/Message.h"
#include "Application/Common/ClientRequest.h"

class SDBPRequestMessage : public Message
{
public:
	ClientRequest*	request;
	
	bool			Read(ReadBuffer& buffer);
	bool			Write(Buffer& buffer);
};

#endif
