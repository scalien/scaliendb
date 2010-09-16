#include "ClientResponse.h"
#include "System/Buffers/Buffer.h"

bool ClientResponse::OK(uint64_t commandID_)
{
	type = CLIENTRESPONSE_OK;
	commandID = commandID_;
	return true;
}

bool ClientResponse::Number(uint64_t commandID_, uint64_t number_)
{
	type = CLIENTRESPONSE_NUMBER;
	commandID = commandID_;
	number = number_;
	return true;
}

bool ClientResponse::Value(uint64_t commandID_, ReadBuffer& value_)
{
	type = CLIENTRESPONSE_VALUE;
	commandID = commandID_;
	value = value_;
	return true;
}

bool ClientResponse::GetConfigStateResponse(uint64_t commandID_, ConfigState* configState_)
{
	type = CLIENTRESPONSE_GET_CONFIG_STATE;
	commandID = commandID_;
	configState = configState_;
	return true;
}

bool ClientResponse::NotMaster(uint64_t commandID_)
{
	type = CLIENTRESPONSE_NOTMASTER;
	commandID = commandID_;
	return true;
}

bool ClientResponse::Failed(uint64_t commandID_)
{
	type = CLIENTRESPONSE_FAILED;
	commandID = commandID_;
	return true;
}

bool ClientResponse::Read(ReadBuffer& buffer)
{
	int			read;
		
	if (buffer.GetLength() < 1)
		return false;
	
	switch (buffer.GetCharAt(0))
	{
		case CLIENTRESPONSE_OK:
			read = buffer.Readf("%c:%U",
			 &type, &commandID);
			break;
		case CLIENTRESPONSE_NUMBER:
			read = buffer.Readf("%c:%U:%U",
			 &type, &commandID, &number);
			break;
		case CLIENTRESPONSE_VALUE:
			read = buffer.Readf("%c:%U:%#R",
			 &type, &commandID, &value);
			break;
		case CLIENTRESPONSE_GET_CONFIG_STATE:
			if (configState)
				delete configState;
			configState = new ConfigState;
			return configState->Read(buffer, true);
		case CLIENTRESPONSE_NOTMASTER:
			read = buffer.Readf("%c:%U",
			 &type, &commandID);
			break;
		case CLIENTRESPONSE_FAILED:
			read = buffer.Readf("%c:%U",
			 &type, &commandID);
			break;
		default:
			return false;
	}
	
	return (read == (signed)buffer.GetLength() ? true : false);
}

bool ClientResponse::Write(Buffer& buffer)
{
	switch (type)
	{
		case CLIENTRESPONSE_OK:
			buffer.Writef("%c:%U",
			 type, commandID);
			return true;
		case CLIENTRESPONSE_NUMBER:
			buffer.Writef("%c:%U:%U",
			 type, commandID, number);
			return true;
		case CLIENTRESPONSE_VALUE:
			buffer.Writef("%c:%U:%#R",
			 type, commandID, &value);
			return true;
		case CLIENTRESPONSE_GET_CONFIG_STATE:
			return configState->Write(buffer, true);
		case CLIENTRESPONSE_NOTMASTER:
			buffer.Writef("%c:%U",
			 type, commandID);
			return true;
		case CLIENTRESPONSE_FAILED:
			buffer.Writef("%c:%U",
			 type, commandID);
			return true;
		default:
			return false;
	}
	
	return true;
}
