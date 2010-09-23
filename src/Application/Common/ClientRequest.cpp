#include "ClientRequest.h"
#include "System/Buffers/Buffer.h"

ClientRequest::ClientRequest()
{
    session = NULL;
    response.request = this;
    prev = next = this;
}

void ClientRequest::OnComplete(bool last)
{
    if (!session)
        ASSERT_FAIL();
    session->OnComplete(this, last);
}

bool ClientRequest::IsControllerRequest()
{
    if (type == CLIENTREQUEST_GET_MASTER        ||
        type == CLIENTREQUEST_GET_CONFIG_STATE  ||
        type == CLIENTREQUEST_CREATE_QUORUM     ||
        type == CLIENTREQUEST_CREATE_DATABASE   ||
        type == CLIENTREQUEST_RENAME_DATABASE   ||
        type == CLIENTREQUEST_DELETE_DATABASE   ||
        type == CLIENTREQUEST_CREATE_TABLE      ||
        type == CLIENTREQUEST_RENAME_TABLE      ||
        type == CLIENTREQUEST_DELETE_TABLE)
            return true;

    return false;
}

bool ClientRequest::IsShardServerRequest()
{
    if (type == CLIENTREQUEST_GET       ||
        type == CLIENTREQUEST_SET       ||
        type == CLIENTREQUEST_DELETE)
            return true;
    
    return false;
}

bool ClientRequest::IsSafeRequest()
{
    return true;
}

bool ClientRequest::GetMaster(uint64_t commandID_)
{
    type = CLIENTREQUEST_GET_MASTER;
    commandID = commandID_;
    return true;
}

bool ClientRequest::GetConfigState(uint64_t commandID_)
{
    type = CLIENTREQUEST_GET_CONFIG_STATE;
    commandID = commandID_;
    return true;
}

bool ClientRequest::CreateQuorum(uint64_t commandID_, char productionType_, NodeList nodes_)
{
    type = CLIENTREQUEST_CREATE_QUORUM;
    commandID = commandID_;
    productionType = productionType_;
    nodes = nodes_;
    return true;
}

bool ClientRequest::CreateDatabase(
 uint64_t commandID_, char productionType_, ReadBuffer& name_)
{
    type = CLIENTREQUEST_CREATE_DATABASE;
    commandID = commandID_;
    productionType = productionType_;
    name.Write(name_);
    return true;
}

bool ClientRequest::RenameDatabase(
 uint64_t commandID_, uint64_t databaseID_, ReadBuffer& name_)
{
    type = CLIENTREQUEST_RENAME_DATABASE;
    commandID = commandID_;
    databaseID = databaseID_;
    name.Write(name_);
    return true;
}

bool ClientRequest::DeleteDatabase(
 uint64_t commandID_, uint64_t databaseID_)
{
    type = CLIENTREQUEST_DELETE_DATABASE;
    commandID = commandID_;
    databaseID = databaseID_;
    return true;
}

bool ClientRequest::CreateTable(
 uint64_t commandID_, uint64_t databaseID_, uint64_t quorumID_, ReadBuffer& name_)
{
    type = CLIENTREQUEST_CREATE_TABLE;
    commandID = commandID_;
    databaseID = databaseID_;
    quorumID = quorumID_;
    name.Write(name_);
    return true;
}

bool ClientRequest::RenameTable(
 uint64_t commandID_, uint64_t databaseID_, uint64_t tableID_, ReadBuffer& name_)
{
    type = CLIENTREQUEST_RENAME_TABLE;
    commandID = commandID_;
    databaseID = databaseID_;
    tableID = tableID_;
    name.Write(name_);
    return true;
}

bool ClientRequest::DeleteTable(
 uint64_t commandID_, uint64_t databaseID_, uint64_t tableID_)
{
    type = CLIENTREQUEST_DELETE_TABLE;
    commandID = commandID_;
    databaseID = databaseID_;
    tableID = tableID_;
    return true;
}

bool ClientRequest::Set(
 uint64_t commandID_, uint64_t databaseID_, uint64_t tableID_, ReadBuffer& key_, ReadBuffer& value_)
{
    type = CLIENTREQUEST_SET;
    commandID = commandID_;
    databaseID = databaseID_;
    tableID = tableID_;
    key.Write(key_);
    value.Write(value_);
    return true;
}

bool ClientRequest::Get(
 uint64_t commandID_, uint64_t databaseID_, uint64_t tableID_, ReadBuffer& key_)
{
    type = CLIENTREQUEST_GET;
    commandID = commandID_;
    databaseID = databaseID_;
    tableID = tableID_;
    key.Write(key_);
    return true;
}

bool ClientRequest::Delete(
 uint64_t commandID_, uint64_t databaseID_, uint64_t tableID_, ReadBuffer& key_)
{
    type = CLIENTREQUEST_DELETE;
    commandID = commandID_;
    databaseID = databaseID_;
    tableID = tableID_;
    key.Write(key_);
    return true;
}
