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
	ReadBuffer		key;
	ReadBuffer		value;
	
	void			Get(ReadBuffer& key);
	void			Set(ReadBuffer& key, ReadBuffer& value);
	
	DataMessage*	prev;
	DataMessage*	next;
	void*			ptr;

	// implementation of Message interface:
	bool			Read(const ReadBuffer& buffer);
	bool			Write(Buffer& buffer);
};

#endif
