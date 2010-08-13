#include "DataMessage.h"

DataMessage::DataMessage()
{
	prev = this;
	next = this;
}

void DataMessage::Get(ReadBuffer& key_)
{
	type = DATAMESSAGE_GET;
	key = key_;
}

void DataMessage::Set(ReadBuffer& key_, ReadBuffer& value_)
{
	type = DATAMESSAGE_SET;
	key = key_;
	value = value_;
}

bool DataMessage::Read(ReadBuffer& buffer)
{
	int		read;
	
	if (buffer.GetLength() < 1)
		return false;

	switch (buffer.GetCharAt(0))
	{
		case DATAMESSAGE_GET:
			ASSERT_FAIL();
			break;
		case DATAMESSAGE_SET:
			read = buffer.Readf("%c:%M:%M",
			 &type, &key, &value);
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
		case DATAMESSAGE_GET:
			ASSERT_FAIL();
			break;
		case DATAMESSAGE_SET:
			return buffer.Writef("%c:%M:%M",
			 type, &key, &value);
			break;
		default:
			return false;
	}
}
