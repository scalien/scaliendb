#ifndef DATAMESSAGE_H
#define DATAMESSAGE_H

#include "Framework/Messaging/Message.h"

#define DATAMESSAGE_SET			'S'
#define DATAMESSAGE_DELETE		'D'

/*
===============================================================================================

 DataMessage

===============================================================================================
*/

class DataMessage : public Message
{
	// Variables
	char			type;
	Buffer			key;
	Buffer			value;

	// Data management
	void			Set(ReadBuffer key, ReadBuffer value);
	void			Delete(ReadBuffer key);

	// For InList<>
	DataMessage*	prev;
	DataMessage*	next;

	// Serialization
	bool			Read(ReadBuffer& buffer);
	bool			Write(Buffer& buffer);
};

#endif
