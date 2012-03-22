#include "ClientResponse.h"
#include "System/Buffers/Buffer.h"

ClientResponse::ClientResponse()
{
    Init();
    configState.SetAutoCreate(true);
}

ClientResponse::~ClientResponse()
{
    delete valueBuffer;
    delete[] keys;
    delete[] values;
}

void ClientResponse::Init()
{
    request = NULL;
    valueBuffer = NULL;
    keys = NULL;
    values = NULL;
    type = CLIENTRESPONSE_NORESPONSE;
    numKeys = 0;
    snumber = 0;
    number = 0;
    paxosID = 0;
    value.Reset();
    isConditionalSuccess = false;
}

void ClientResponse::Clear()
{
    if (valueBuffer != NULL)
        delete valueBuffer;
    valueBuffer = NULL;
    
    if (keys != NULL)
        delete[] keys;
    keys = NULL;
    
    if (values != NULL)
        delete[] values;
    values = NULL;

    configState.Free();
    Init();
}

void ClientResponse::CopyValue()
{
    if (valueBuffer == NULL)
        valueBuffer = new Buffer;

    valueBuffer->Write(value.GetBuffer(), value.GetLength());
    value.Wrap(*valueBuffer);
}

void ClientResponse::CopyKeys()
{
    unsigned    i;
    char*       p;
 
    if (valueBuffer == NULL)
        valueBuffer = new Buffer;
    
    valueBuffer->Clear();
    for (i = 0; i < numKeys; i++)
        valueBuffer->Append(keys[i]);

    p = valueBuffer->GetBuffer();
    for (i = 0; i < numKeys; i++)
    {
        keys[i].SetBuffer(p);
        p += keys[i].GetLength();
    }
}

void ClientResponse::CopyKeyValues()
{
    unsigned    i;
    char*       p;
    
    if (valueBuffer == NULL)
        valueBuffer = new Buffer;
    
    valueBuffer->Clear();
    for (i = 0; i < numKeys; i++)
    {
        valueBuffer->Append(keys[i]);
        valueBuffer->Append(values[i]);
    }
    
    p = valueBuffer->GetBuffer();
    for (i = 0; i < numKeys; i++)
    {
        keys[i].SetBuffer(p);
        p += keys[i].GetLength();
        values[i].SetBuffer(p);
        p += values[i].GetLength();
    }
}

void ClientResponse::Transfer(ClientResponse& other)
{
    other.type = type;
    other.snumber = snumber;
    other.number = number;
    other.commandID = commandID;
    other.value = value;
    other.valueBuffer = valueBuffer;
    configState.Transfer(other.configState);
    other.numKeys = numKeys;
    other.keys = keys;
    other.values = values;
    other.isConditionalSuccess = isConditionalSuccess;

    Init();
}

bool ClientResponse::OK()
{
    type = CLIENTRESPONSE_OK;
    return true;
}

bool ClientResponse::SignedNumber(int64_t snumber_)
{
    type = CLIENTRESPONSE_SNUMBER;
    snumber = snumber_;
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

bool ClientResponse::ListKeys(unsigned numKeys_, ReadBuffer* keys_)
{
    unsigned    i;
    
    type = CLIENTRESPONSE_LIST_KEYS;
    numKeys = numKeys_;
    delete[] keys;
    keys = new ReadBuffer[numKeys];
    for (i = 0; i < numKeys; i++)
        keys[i] = keys_[i];
    
    return true;
}

bool ClientResponse::ListKeyValues(unsigned numKeys_, ReadBuffer* keys_, ReadBuffer* values_)
{
    unsigned    i;
    
    type = CLIENTRESPONSE_LIST_KEYVALUES;
    numKeys = numKeys_;
    delete[] keys;
    keys = new ReadBuffer[numKeys];
    delete[] values;
    values = new ReadBuffer[numKeys];
    for (i = 0; i < numKeys; i++)
    {
        keys[i] = keys_[i];
        values[i] = values_[i];
    }
    
    return true;
}

bool ClientResponse::ConfigStateResponse(ConfigState& configState_)
{
    type = CLIENTRESPONSE_CONFIG_STATE;
    configState.Set(new ConfigState);
    *configState.Get() = configState_;
    return true;
}

bool ClientResponse::NoService()
{
    type = CLIENTRESPONSE_NOSERVICE;
    return true;
}

bool ClientResponse::BadSchema()
{
    type = CLIENTRESPONSE_BADSCHEMA;
    return true;
}

bool ClientResponse::Failed()
{
    type = CLIENTRESPONSE_FAILED;
    return true;
}

bool ClientResponse::NoResponse()
{
    type = CLIENTRESPONSE_NORESPONSE;
    return true;
}

bool ClientResponse::Hello()
{
    type = CLIENTRESPONSE_HELLO;
    return true;
}

bool ClientResponse::Next(
 ReadBuffer& nextShardKey, ReadBuffer& endKey_, ReadBuffer& prefix_,
 uint64_t count)
{
    type = CLIENTRESPONSE_NEXT;
    value = nextShardKey;
    endKey = endKey_;
    prefix = prefix_;
    number = count;
    return true;
}

void ClientResponse::SetConditionalSuccess(bool isConditionalSuccess_)
{
    isConditionalSuccess = isConditionalSuccess_;
}
