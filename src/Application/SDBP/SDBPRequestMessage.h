#ifndef SDBPCLIENTREQUEST_H
#define SDBPCLIENTREQUEST_H

#include "Framework/Messaging/Message.h"
#include "Application/Common/ClientRequest.h"

class SDBPClientRequest : public Message
{
public:
	ClientRequest*	request;
	
	bool			Read(ReadBuffer& buffer);
	bool			Write(Buffer& buffer);
};

#endif
