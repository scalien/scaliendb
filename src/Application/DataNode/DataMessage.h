#ifndef DATAMESSAGE_H
#define DATAMESSAGE_H

#include "Framework/Messaging/Message.h"

#define DATAMESSAGE_GET		'G'
#define DATAMESSAGE_SET		'S'


class DataMessage : public Message
{
public:
	DataMessage();

	char			type;
	Buffer			key;
	Buffer			value;
	
	void			Get(Buffer& key);
	void			Set(Buffer& key, Buffer& value);
	
	DataMessage*	prev;
	DataMessage*	next;
	void*			ptr;

	// implementation of Message interface:
	bool			Read(const ReadBuffer& buffer);
	bool			Write(Buffer& buffer);
};

#endif
