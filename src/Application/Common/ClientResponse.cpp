#include "ClientResponse.h"
#include "System/Buffers/Buffer.h"

ClientResponse::ClientResponse()
{
	configState = NULL;
}

ClientResponse::~ClientResponse()
{
	delete configState;
}

ConfigState* ClientResponse::TransferConfigState()
{
	ConfigState*	ret;
	
	ret = configState;
	configState = NULL;
	return ret;
}

bool ClientResponse::OK()
{
	type = CLIENTRESPONSE_OK;
	return true;
}

bool ClientResponse::Number(uint64_t number_)
{
	type = CLIENTRESPONSE_NUMBER;
	number = number_;
	return true;
}

bool ClientResponse::Value(ReadBuffer& value_)
{
	type = CLIENTRESPONSE_VALUE;
	value = value_;
	return true;
}

bool ClientResponse::GetConfigStateResponse(ConfigState* configState_)
{
	type = CLIENTRESPONSE_GET_CONFIG_STATE;
	configState = configState_;
	return true;
}

bool ClientResponse::NotMaster()
{
	type = CLIENTRESPONSE_NOSERVICE;
	return true;
}

bool ClientResponse::Failed()
{
	type = CLIENTRESPONSE_FAILED;
	return true;
}
