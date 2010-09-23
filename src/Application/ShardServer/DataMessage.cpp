#include "DataMessage.h"

void DataMessage::Set(ReadBuffer key_, ReadBuffer value_)
{
	type = DATAMESSAGE_SET;
	key.Write(key_);
	value.Write(value_);
}

void DataMessage::Delete(ReadBuffer key_)
{
	type = DATAMESSAGE_DELETE;
	key.Write(key_);
}

bool DataMessage::Read(ReadBuffer& buffer)
{
	int read;
	
	if (buffer.GetLength() < 1)
		return false;
	
	switch (buffer.GetCharAt(0))
	{
		// Data management
		case DATAMESSAGE_SET:
			read = buffer.Readf("%c:%#B:%#B",
			 &type, &key, &value);
			break;
		case DATAMESSAGE_DELETE:
			read = buffer.Readf("%c:%#B",
			 &type, &key);
			break;
		default:
			return false;
	}
	
	return (read == (signed)buffer.GetLength() ? true : false);
}

bool DataMessage::Write(Buffer& buffer)
{
	switch (type)
	{
		// Cluster management
		case DATAMESSAGE_SET:
			buffer.Writef("%c:%#B:%#B",
			 type, &key, &value);
			break;
		case DATAMESSAGE_DELETE:
			buffer.Writef("%c:%#B",
			 type, &key);
			break;
		default:
			return false;
	}
	
	return true;
}
