#include "SDBPClientResponse.h"
#include "Application/Common/ClientRequest.h"

bool SDBPClientResponse::Read(ReadBuffer& buffer)
{
	int			read;
		
	if (buffer.GetLength() < 1)
		return false;
	
	switch (buffer.GetCharAt(0))
	{
		case CLIENTRESPONSE_OK:
			read = buffer.Readf("%c:%U",
			 &response->type, &response->request->commandID);
			break;
		case CLIENTRESPONSE_NUMBER:
			read = buffer.Readf("%c:%U:%U",
			 &response->type, &response->request->commandID, &response->number);
			break;
		case CLIENTRESPONSE_VALUE:
			read = buffer.Readf("%c:%U:%#R",
			 &response->type, &response->request->commandID, &response->value);
			break;
		case CLIENTRESPONSE_GET_CONFIG_STATE:
			if (response->configState)
				delete response->configState;
			response->configState = new ConfigState;
			return response->configState->Read(buffer, true);
		case CLIENTRESPONSE_NOTMASTER:
			read = buffer.Readf("%c:%U",
			 &response->type, &response->request->commandID);
			break;
		case CLIENTRESPONSE_FAILED:
			read = buffer.Readf("%c:%U",
			 &response->type, &response->request->commandID);
			break;
		default:
			return false;
	}
	
	return (read == (signed)buffer.GetLength() ? true : false);
}

bool SDBPClientResponse::Write(Buffer& buffer)
{
	switch (response->type)
	{
		case CLIENTRESPONSE_OK:
			buffer.Writef("%c:%U",
			 response->type, response->request->commandID);
			return true;
		case CLIENTRESPONSE_NUMBER:
			buffer.Writef("%c:%U:%U",
			 response->type, response->request->commandID, response->number);
			return true;
		case CLIENTRESPONSE_VALUE:
			buffer.Writef("%c:%U:%#R",
			 response->type, response->request->commandID, &response->value);
			return true;
		case CLIENTRESPONSE_GET_CONFIG_STATE:
			return response->configState->Write(buffer, true);
		case CLIENTRESPONSE_NOTMASTER:
			buffer.Writef("%c:%U",
			 response->type, response->request->commandID);
			return true;
		case CLIENTRESPONSE_FAILED:
			buffer.Writef("%c:%U",
			 response->type, response->request->commandID);
			return true;
		default:
			return false;
	}
	
	return true;
}
