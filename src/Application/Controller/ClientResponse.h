#ifndef CLIENTRESPONSE_H
#define CLIENTRESPONSE_H

#include "System/Platform.h"
#include "System/Buffers/ReadBuffer.h"

#define CLIENTRESPONSE_OK				'O'
#define CLIENTRESPONSE_VALUE			'V'
#define CLIENTRESPONSE_NOTMASTER		'N'
#define CLIENTRESPONSE_FAILED			'F'

/*
===============================================================================

 ClientResponse

===============================================================================
*/

class ClientResponse
{
public:
	/* Variables */
	char		type;
	uint64_t	commandID;
	ReadBuffer	value;
	
	/* Responses */
	bool		OK(uint64_t commandID);
	bool		Value(uint64_t commandID, ReadBuffer& value);
	bool		NotMaster(uint64_t commandID);
	bool		Failed(uint64_t commandID);

	/* Serialization */
	bool		Read(ReadBuffer buffer);
	bool		Write(Buffer& buffer);
};

#endif
