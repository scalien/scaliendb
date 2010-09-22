#ifndef SDBPCLIENTRESPONSE_H
#define SDBPCLIENTRESPONSE_H

#include "Framework/Messaging/Message.h"
#include "Application/Common/ClientResponse.h"

class SDBPClientResponse : public Message
{
public:
	ClientResponse*	response;
	
	bool			Read(ReadBuffer& buffer);
	bool			Write(Buffer& buffer);
};

#endif
