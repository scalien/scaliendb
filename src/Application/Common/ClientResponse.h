#ifndef CLIENTRESPONSE_H
#define CLIENTRESPONSE_H

#include "Framework/Messaging/Message.h"
#include "Application/Controller/State/ConfigState.h"

#define CLIENTRESPONSE_OK				'O'
#define CLIENTRESPONSE_NUMBER			'n'
#define CLIENTRESPONSE_VALUE			'V'
#define CLIENTRESPONSE_GET_CONFIG_STATE	'C'
#define CLIENTRESPONSE_NOTMASTER		'N'
#define CLIENTRESPONSE_FAILED			'F'

/*
===============================================================================

 ClientResponse

===============================================================================
*/

class ClientResponse : public Message
{
public:
	/* Variables */
	char			type;
	uint64_t		commandID;
	uint64_t		number;
	ReadBuffer		value;
	ConfigState*	configState;
	
	/* Responses */
	bool		OK(uint64_t commandID);
	bool		Number(uint64_t commandID, uint64_t number);
	bool		Value(uint64_t commandID, ReadBuffer& value);
	bool		GetConfigStateResponse(uint64_t commandID, ConfigState* configState);
	bool		NotMaster(uint64_t commandID);
	bool		Failed(uint64_t commandID);

	/* Serialization */
	bool		Read(ReadBuffer& buffer);
	bool		Write(Buffer& buffer);	
};

#endif
