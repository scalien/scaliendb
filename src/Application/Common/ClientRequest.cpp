#include "ClientRequest.h"
#include "System/Buffers/Buffer.h"

ClientRequest::ClientRequest()
{
    prev = next = this;
    Init();
}

void ClientRequest::Init()
{
    session = NULL;
    response.request = this;
    prev = next = this;
    isBulk = false;

    response.NoResponse();
    name.Clear();
    key.Clear();
    value.Clear();
    nodes.Clear();
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
        type == CLIENTREQUEST_DELETE_QUORUM     ||
        type == CLIENTREQUEST_ADD_NODE          ||
        type == CLIENTREQUEST_REMOVE_NODE       ||
        type == CLIENTREQUEST_ACTIVATE_NODE     ||
        type == CLIENTREQUEST_CREATE_DATABASE   ||
        type == CLIENTREQUEST_RENAME_DATABASE   ||
        type == CLIENTREQUEST_DELETE_DATABASE   ||
        type == CLIENTREQUEST_CREATE_TABLE      ||
        type == CLIENTREQUEST_RENAME_TABLE      ||
        type == CLIENTREQUEST_DELETE_TABLE      ||
        type == CLIENTREQUEST_TRUNCATE_TABLE    ||
        type == CLIENTREQUEST_FREEZE_TABLE      ||
        type == CLIENTREQUEST_UNFREEZE_TABLE    ||
        type == CLIENTREQUEST_SPLIT_SHARD       ||
        type == CLIENTREQUEST_MIGRATE_SHARD)
            return true;

    return false;
}

bool ClientRequest::IsShardServerRequest()
{
    if (type == CLIENTREQUEST_BULK_LOADING      ||
        type == CLIENTREQUEST_GET               ||
        type == CLIENTREQUEST_SET               ||
        type == CLIENTREQUEST_SET_IF_NOT_EXISTS ||
        type == CLIENTREQUEST_TEST_AND_SET      ||
        type == CLIENTREQUEST_GET_AND_SET       ||
        type == CLIENTREQUEST_ADD               ||
        type == CLIENTREQUEST_APPEND            ||
        type == CLIENTREQUEST_DELETE            ||
        type == CLIENTREQUEST_REMOVE            ||
        type == CLIENTREQUEST_SUBMIT            ||
        type == CLIENTREQUEST_LIST_KEYS         ||
        type == CLIENTREQUEST_LIST_KEYVALUES    ||
        type == CLIENTREQUEST_COUNT)
            return true;
    
    return false;
}

bool ClientRequest::IsSafeRequest()
{
    return true;
}

bool ClientRequest::IsList()
{
    if (type == CLIENTREQUEST_LIST_KEYS         ||
        type == CLIENTREQUEST_LIST_KEYVALUES    ||
        type == CLIENTREQUEST_COUNT)
            return true;
    
    return false;
}

bool ClientRequest::GetMaster(uint64_t commandID_)
{
    type = CLIENTREQUEST_GET_MASTER;
    commandID = commandID_;
    return true;
}

bool ClientRequest::GetConfigState(uint64_t commandID_, uint64_t changeTimeout_)
{
    type = CLIENTREQUEST_GET_CONFIG_STATE;
    commandID = commandID_;
    changeTimeout = changeTimeout_;
    return true;
}

bool ClientRequest::CreateQuorum(uint64_t commandID_, List<uint64_t>& nodes_)
{
    type = CLIENTREQUEST_CREATE_QUORUM;
    commandID = commandID_;
    nodes = nodes_;
    return true;
}

bool ClientRequest::DeleteQuorum(uint64_t commandID_, uint64_t quorumID_)
{
    type = CLIENTREQUEST_DELETE_QUORUM;
    commandID = commandID_;
    quorumID = quorumID_;
    return true;
}

bool ClientRequest::AddNode(uint64_t commandID_, uint64_t quorumID_, uint64_t nodeID_)
{
    type = CLIENTREQUEST_ADD_NODE;
    commandID = commandID_;
    quorumID = quorumID_;
    nodeID = nodeID_;
    return true;
}

bool ClientRequest::RemoveNode(uint64_t commandID_, uint64_t quorumID_, uint64_t nodeID_)
{
    type = CLIENTREQUEST_REMOVE_NODE;
    commandID = commandID_;
    quorumID = quorumID_;
    nodeID = nodeID_;
    return true;
}

bool ClientRequest::ActivateNode(uint64_t commandID_, uint64_t nodeID_)
{
    type = CLIENTREQUEST_ACTIVATE_NODE;
    commandID = commandID_;
    nodeID = nodeID_;
    return true;
}

bool ClientRequest::CreateDatabase(
 uint64_t commandID_, ReadBuffer& name_)
{
    type = CLIENTREQUEST_CREATE_DATABASE;
    commandID = commandID_;
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
 uint64_t commandID_, uint64_t tableID_, ReadBuffer& name_)
{
    type = CLIENTREQUEST_RENAME_TABLE;
    commandID = commandID_;
    tableID = tableID_;
    name.Write(name_);
    return true;
}

bool ClientRequest::DeleteTable(
 uint64_t commandID_, uint64_t tableID_)
{
    type = CLIENTREQUEST_DELETE_TABLE;
    commandID = commandID_;
    tableID = tableID_;
    return true;
}

bool ClientRequest::TruncateTable(
 uint64_t commandID_, uint64_t tableID_)
{
    type = CLIENTREQUEST_TRUNCATE_TABLE;
    commandID = commandID_;
    tableID = tableID_;
    return true;
}

bool ClientRequest::FreezeTable(
 uint64_t commandID_, uint64_t tableID_)
{
    type = CLIENTREQUEST_FREEZE_TABLE;
    commandID = commandID_;
    tableID = tableID_;
    return true;
}

bool ClientRequest::UnfreezeTable(
 uint64_t commandID_, uint64_t tableID_)
{
    type = CLIENTREQUEST_UNFREEZE_TABLE;
    commandID = commandID_;
    tableID = tableID_;
    return true;
}

bool ClientRequest::SplitShard(
 uint64_t commandID_, uint64_t shardID_, ReadBuffer& key_)
{
    type = CLIENTREQUEST_SPLIT_SHARD;
    commandID = commandID_;
    shardID = shardID_;
    key.Write(key_);
    return true;
}

bool ClientRequest::MigrateShard(
 uint64_t commandID_, uint64_t quorumID_, uint64_t shardID_)
{
    type = CLIENTREQUEST_MIGRATE_SHARD;
    commandID = commandID_;
    shardID = shardID_;
    quorumID = quorumID_;
    return true;
}

bool ClientRequest::Get(
 uint64_t commandID_, uint64_t tableID_, ReadBuffer& key_)
{
    type = CLIENTREQUEST_GET;
    commandID = commandID_;
    tableID = tableID_;
    key.Write(key_);
    return true;
}

bool ClientRequest::Set(
 uint64_t commandID_, uint64_t tableID_, ReadBuffer& key_, ReadBuffer& value_)
{
    type = CLIENTREQUEST_SET;
    commandID = commandID_;
    tableID = tableID_;
    key.Write(key_);
    value.Write(value_);
    return true;
}

bool ClientRequest::SetIfNotExists(
 uint64_t commandID_, uint64_t tableID_, ReadBuffer& key_, ReadBuffer& value_)
{
    type = CLIENTREQUEST_SET_IF_NOT_EXISTS;
    commandID = commandID_;
    tableID = tableID_;
    key.Write(key_);
    value.Write(value_);
    return true;
}

bool ClientRequest::TestAndSet(
 uint64_t commandID_, uint64_t tableID_,
 ReadBuffer& key_, ReadBuffer& test_, ReadBuffer& value_)
{
    type = CLIENTREQUEST_TEST_AND_SET;
    commandID = commandID_;
    tableID = tableID_;
    key.Write(key_);
    test.Write(test_);
    value.Write(value_);
    return true;
}

bool ClientRequest::GetAndSet(
 uint64_t commandID_, uint64_t tableID_,
 ReadBuffer& key_, ReadBuffer& value_)
{
    type = CLIENTREQUEST_GET_AND_SET;
    commandID = commandID_;
    tableID = tableID_;
    key.Write(key_);
    value.Write(value_);
    return true;
}

bool ClientRequest::Add(
 uint64_t commandID_, uint64_t tableID_,
 ReadBuffer& key_, int64_t number_)
{
    type = CLIENTREQUEST_ADD;
    commandID = commandID_;
    tableID = tableID_;
    key.Write(key_);
    number = number_;
    return true;
}

bool ClientRequest::Append(
 uint64_t commandID_, uint64_t tableID_, ReadBuffer& key_, ReadBuffer& value_)
{
    type = CLIENTREQUEST_APPEND;
    commandID = commandID_;
    tableID = tableID_;
    key.Write(key_);
    value.Write(value_);
    return true;
}

bool ClientRequest::Delete(
 uint64_t commandID_, uint64_t tableID_, ReadBuffer& key_)
{
    type = CLIENTREQUEST_DELETE;
    commandID = commandID_;
    tableID = tableID_;
    key.Write(key_);
    return true;
}

bool ClientRequest::Remove(
 uint64_t commandID_, uint64_t tableID_, ReadBuffer& key_)
{
    type = CLIENTREQUEST_REMOVE;
    commandID = commandID_;
    tableID = tableID_;
    key.Write(key_);
    return true;
}

bool ClientRequest::ListKeys(
 uint64_t commandID_, uint64_t tableID_, ReadBuffer& startKey_, unsigned count_, unsigned offset_)
{
    type = CLIENTREQUEST_LIST_KEYS;
    commandID = commandID_;
    tableID = tableID_;
    key.Write(startKey_);
    count = count_;
    offset = offset_;
    return true;
}

bool ClientRequest::ListKeyValues(
 uint64_t commandID_, uint64_t tableID_, ReadBuffer& startKey_, unsigned count_, unsigned offset_)
{
    type = CLIENTREQUEST_LIST_KEYVALUES;
    commandID = commandID_;
    tableID = tableID_;
    key.Write(startKey_);
    count = count_;
    offset = offset_;
    return true;
}

bool ClientRequest::Count(
 uint64_t commandID_, uint64_t tableID_, ReadBuffer& startKey_, unsigned count_, unsigned offset_)
{
    type = CLIENTREQUEST_COUNT;
    commandID = commandID_;
    tableID = tableID_;
    key.Write(startKey_);
    count = count_;
    offset = offset_;
    return true;
}

bool ClientRequest::Submit(
 uint64_t quorumID_)
{
    type = CLIENTREQUEST_SUBMIT;
    quorumID = quorumID_;
    return true;
}

bool ClientRequest::BulkLoading(uint64_t commandID_)
{
    type = CLIENTREQUEST_BULK_LOADING;
    commandID = commandID_;
    return true;
}
