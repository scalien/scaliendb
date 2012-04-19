#include "ClientRequest.h"
#include "ClientSession.h"
#include "System/Buffers/Buffer.h"

ClientRequest::ClientRequest()
{
    prev = next = this;
    Init();
}

void ClientRequest::Init()
{
    ASSERT(prev == this);
    ASSERT(next == this);
    session = NULL;
    response.request = this;
    forwardDirection = false;
    findByLastKey = false;
    transactional = false;
    type = CLIENTREQUEST_UNDEFINED;
    commandID = 0;
    quorumID = 0;
    databaseID = 0;
    tableID = 0;
    shardID = 0;
    nodeID = 0;
    paxosID = 0;
    configPaxosID = 0;
    priority = 0;
    number = 0;
    count = 0;
    changeTimeout = 0;
    lastChangeTime = 0;

    response.NoResponse();
    name.Clear();
    key.Clear();
    value.Clear();
    nodes.Clear();
}

void ClientRequest::Clear()
{
    response.Clear();
    nodes.Clear();
    Init();
}

void ClientRequest::OnComplete(bool last)
{
    if (!session)
        ASSERT_FAIL();
    session->OnComplete(this, last);
}

bool ClientRequest::IsControllerRequest()
{
    if (type == CLIENTREQUEST_GET_MASTER                        ||
        type == CLIENTREQUEST_GET_MASTER_HTTP                   ||
        type == CLIENTREQUEST_GET_CONFIG_STATE                  ||
        type == CLIENTREQUEST_UNREGISTER_SHARDSERVER            ||
        type == CLIENTREQUEST_CREATE_QUORUM                     ||
        type == CLIENTREQUEST_RENAME_QUORUM                     ||
        type == CLIENTREQUEST_DELETE_QUORUM                     ||
        type == CLIENTREQUEST_ADD_SHARDSERVER_TO_QUORUM         ||
        type == CLIENTREQUEST_REMOVE_SHARDSERVER_FROM_QUORUM    ||
        type == CLIENTREQUEST_ACTIVATE_SHARDSERVER              ||
        type == CLIENTREQUEST_SET_PRIORITY                      ||
        type == CLIENTREQUEST_CREATE_DATABASE                   ||
        type == CLIENTREQUEST_RENAME_DATABASE                   ||
        type == CLIENTREQUEST_DELETE_DATABASE                   ||
        type == CLIENTREQUEST_CREATE_TABLE                      ||
        type == CLIENTREQUEST_RENAME_TABLE                      ||
        type == CLIENTREQUEST_DELETE_TABLE                      ||
        type == CLIENTREQUEST_TRUNCATE_TABLE                    ||
        type == CLIENTREQUEST_FREEZE_TABLE                      ||
        type == CLIENTREQUEST_UNFREEZE_TABLE                    ||
        type == CLIENTREQUEST_SPLIT_SHARD                       ||
        type == CLIENTREQUEST_MIGRATE_SHARD)
            return true;

    return false;
}

bool ClientRequest::IsShardServerRequest()
{
    if (type == CLIENTREQUEST_GET                   ||
        type == CLIENTREQUEST_SET                   ||
        type == CLIENTREQUEST_SET_IF_NOT_EXISTS     ||
        type == CLIENTREQUEST_TEST_AND_SET          ||
        type == CLIENTREQUEST_TEST_AND_DELETE       ||
        type == CLIENTREQUEST_GET_AND_SET           ||
        type == CLIENTREQUEST_ADD                   ||
        type == CLIENTREQUEST_APPEND                ||
        type == CLIENTREQUEST_DELETE                ||
        type == CLIENTREQUEST_REMOVE                ||
        type == CLIENTREQUEST_SEQUENCE_SET          ||
        type == CLIENTREQUEST_SEQUENCE_NEXT         ||
        type == CLIENTREQUEST_LIST_KEYS             ||
        type == CLIENTREQUEST_LIST_KEYVALUES        ||
        type == CLIENTREQUEST_COUNT                 ||
        type == CLIENTREQUEST_START_TRANSACTION     ||
        type == CLIENTREQUEST_COMMIT_TRANSACTION    ||
        type == CLIENTREQUEST_ROLLBACK_TRANSACTION)
            return true;
    
    return false;
}

bool ClientRequest::IsReadRequest()
{
    if (type == CLIENTREQUEST_GET || IsList())
        return true;
    return false;
}

bool ClientRequest::IsList()
{
    if (type == CLIENTREQUEST_LIST_KEYS         ||
        type == CLIENTREQUEST_LIST_KEYVALUES    ||
        type == CLIENTREQUEST_COUNT)
            return true;
    
    return false;
}

bool ClientRequest::IsTransaction()
{
    if (type == CLIENTREQUEST_START_TRANSACTION     ||
        type == CLIENTREQUEST_COMMIT_TRANSACTION    ||
        type == CLIENTREQUEST_ROLLBACK_TRANSACTION)
            return true;
    
    return false;
}

bool ClientRequest::IsActive()
{
    if (session && session->IsActive())
        return true;
    else
        return false;
}

void ClientRequest::GetMaster(
 uint64_t commandID_)
{
    type = CLIENTREQUEST_GET_MASTER;
    commandID = commandID_;
}

void ClientRequest::GetMasterHTTP(
 uint64_t commandID_)
{
    type = CLIENTREQUEST_GET_MASTER_HTTP;
    commandID = commandID_;
}

void ClientRequest::GetConfigState(
 uint64_t commandID_, uint64_t changeTimeout_)
{
    type = CLIENTREQUEST_GET_CONFIG_STATE;
    commandID = commandID_;
    changeTimeout = changeTimeout_;
}

void ClientRequest::UnregisterShardServer(
 uint64_t commandID_, uint64_t nodeID_)
{
    type = CLIENTREQUEST_UNREGISTER_SHARDSERVER;
    commandID = commandID_;
    nodeID = nodeID_;
}

void ClientRequest::CreateQuorum(
 uint64_t commandID_, ReadBuffer& name_, List<uint64_t>& nodes_)
{
    type = CLIENTREQUEST_CREATE_QUORUM;
    commandID = commandID_;
    name.Write(name_);
    nodes = nodes_;
}

void ClientRequest::RenameQuorum(
 uint64_t commandID_, uint64_t quorumID_, ReadBuffer& name_)
{
    type = CLIENTREQUEST_RENAME_QUORUM;
    commandID = commandID_;
    quorumID = quorumID_;
    name.Write(name_);
}

void ClientRequest::DeleteQuorum(
 uint64_t commandID_, uint64_t quorumID_)
{
    type = CLIENTREQUEST_DELETE_QUORUM;
    commandID = commandID_;
    quorumID = quorumID_;
}

void ClientRequest::AddShardServerToQuorum(
 uint64_t commandID_, uint64_t quorumID_, uint64_t nodeID_)
{
    type = CLIENTREQUEST_ADD_SHARDSERVER_TO_QUORUM;
    commandID = commandID_;
    quorumID = quorumID_;
    nodeID = nodeID_;
}

void ClientRequest::RemoveShardServerFromQuorum(
 uint64_t commandID_, uint64_t quorumID_, uint64_t nodeID_)
{
    type = CLIENTREQUEST_REMOVE_SHARDSERVER_FROM_QUORUM;
    commandID = commandID_;
    quorumID = quorumID_;
    nodeID = nodeID_;
}

void ClientRequest::ActivateShardServer(
 uint64_t commandID_, uint64_t nodeID_)
{
    type = CLIENTREQUEST_ACTIVATE_SHARDSERVER;
    commandID = commandID_;
    nodeID = nodeID_;
}

void ClientRequest::SetPriority(
 uint64_t commandID_, uint64_t quorumID_,
 uint64_t nodeID_, uint64_t priority_)
{
    type = CLIENTREQUEST_SET_PRIORITY;
    commandID = commandID_;
    quorumID = quorumID_;
    nodeID = nodeID_;
    priority = priority_;
}

void ClientRequest::CreateDatabase(
 uint64_t commandID_, ReadBuffer& name_)
{
    type = CLIENTREQUEST_CREATE_DATABASE;
    commandID = commandID_;
    name.Write(name_);
}

void ClientRequest::RenameDatabase(
 uint64_t commandID_, uint64_t databaseID_, ReadBuffer& name_)
{
    type = CLIENTREQUEST_RENAME_DATABASE;
    commandID = commandID_;
    databaseID = databaseID_;
    name.Write(name_);
}

void ClientRequest::DeleteDatabase(
 uint64_t commandID_, uint64_t databaseID_)
{
    type = CLIENTREQUEST_DELETE_DATABASE;
    commandID = commandID_;
    databaseID = databaseID_;
}

void ClientRequest::CreateTable(
 uint64_t commandID_, uint64_t databaseID_,
 uint64_t quorumID_, ReadBuffer& name_)
{
    type = CLIENTREQUEST_CREATE_TABLE;
    commandID = commandID_;
    databaseID = databaseID_;
    quorumID = quorumID_;
    name.Write(name_);
}

void ClientRequest::RenameTable(
 uint64_t commandID_, uint64_t tableID_, ReadBuffer& name_)
{
    type = CLIENTREQUEST_RENAME_TABLE;
    commandID = commandID_;
    tableID = tableID_;
    name.Write(name_);
}

void ClientRequest::DeleteTable(
 uint64_t commandID_, uint64_t tableID_)
{
    type = CLIENTREQUEST_DELETE_TABLE;
    commandID = commandID_;
    tableID = tableID_;
}

void ClientRequest::TruncateTable(
 uint64_t commandID_, uint64_t tableID_)
{
    type = CLIENTREQUEST_TRUNCATE_TABLE;
    commandID = commandID_;
    tableID = tableID_;
}

void ClientRequest::FreezeTable(
 uint64_t commandID_, uint64_t tableID_)
{
    type = CLIENTREQUEST_FREEZE_TABLE;
    commandID = commandID_;
    tableID = tableID_;
}

void ClientRequest::UnfreezeTable(
 uint64_t commandID_, uint64_t tableID_)
{
    type = CLIENTREQUEST_UNFREEZE_TABLE;
    commandID = commandID_;
    tableID = tableID_;
}

void ClientRequest::SplitShard(
 uint64_t commandID_, uint64_t shardID_, ReadBuffer& key_)
{
    type = CLIENTREQUEST_SPLIT_SHARD;
    commandID = commandID_;
    shardID = shardID_;
    key.Write(key_);
}

void ClientRequest::MigrateShard(
 uint64_t commandID_, uint64_t shardID_, uint64_t quorumID_)
{
    type = CLIENTREQUEST_MIGRATE_SHARD;
    commandID = commandID_;
    shardID = shardID_;
    quorumID = quorumID_;
}

void ClientRequest::Get(
 uint64_t commandID_, uint64_t configPaxosID_, uint64_t tableID_,
 ReadBuffer& key_)
{
    type = CLIENTREQUEST_GET;
    commandID = commandID_;
    configPaxosID = configPaxosID_;
    tableID = tableID_;
    key.Write(key_);
}

void ClientRequest::Set(
 uint64_t commandID_, uint64_t configPaxosID_, uint64_t tableID_,
 ReadBuffer& key_, ReadBuffer& value_)
{
    type = CLIENTREQUEST_SET;
    commandID = commandID_;
    configPaxosID = configPaxosID_;
    tableID = tableID_;
    key.Write(key_);
    value.Write(value_);
}

void ClientRequest::SetIfNotExists(
 uint64_t commandID_, uint64_t configPaxosID_, uint64_t tableID_,
 ReadBuffer& key_, ReadBuffer& value_)
{
    type = CLIENTREQUEST_SET_IF_NOT_EXISTS;
    commandID = commandID_;
    configPaxosID = configPaxosID_;
    tableID = tableID_;
    key.Write(key_);
    value.Write(value_);
}

void ClientRequest::TestAndSet(
 uint64_t commandID_, uint64_t configPaxosID_, uint64_t tableID_,
 ReadBuffer& key_, ReadBuffer& test_, ReadBuffer& value_)
{
    type = CLIENTREQUEST_TEST_AND_SET;
    commandID = commandID_;
    configPaxosID = configPaxosID_;
    tableID = tableID_;
    key.Write(key_);
    test.Write(test_);
    value.Write(value_);
}

void ClientRequest::TestAndDelete(
 uint64_t commandID_, uint64_t configPaxosID_, uint64_t tableID_,
 ReadBuffer& key_, ReadBuffer& test_)
{
    type = CLIENTREQUEST_TEST_AND_DELETE;
    commandID = commandID_;
    configPaxosID = configPaxosID_;
    tableID = tableID_;
    key.Write(key_);
    test.Write(test_);
}

void ClientRequest::GetAndSet(
 uint64_t commandID_, uint64_t configPaxosID_, uint64_t tableID_,
 ReadBuffer& key_, ReadBuffer& value_)
{
    type = CLIENTREQUEST_GET_AND_SET;
    commandID = commandID_;
    configPaxosID = configPaxosID_;
    tableID = tableID_;
    key.Write(key_);
    value.Write(value_);
}

void ClientRequest::Add(
 uint64_t commandID_, uint64_t configPaxosID_, uint64_t tableID_,
 ReadBuffer& key_, int64_t number_)
{
    type = CLIENTREQUEST_ADD;
    commandID = commandID_;
    configPaxosID = configPaxosID_;
    tableID = tableID_;
    key.Write(key_);
    number = number_;
}

void ClientRequest::Append(
 uint64_t commandID_, uint64_t configPaxosID_, uint64_t tableID_,
  ReadBuffer& key_, ReadBuffer& value_)
{
    type = CLIENTREQUEST_APPEND;
    commandID = commandID_;
    configPaxosID = configPaxosID_;
    tableID = tableID_;
    key.Write(key_);
    value.Write(value_);
}

void ClientRequest::Delete(
 uint64_t commandID_, uint64_t configPaxosID_, uint64_t tableID_,
 ReadBuffer& key_)
{
    type = CLIENTREQUEST_DELETE;
    commandID = commandID_;
    configPaxosID = configPaxosID_;
    tableID = tableID_;
    key.Write(key_);
}

void ClientRequest::Remove(
 uint64_t commandID_, uint64_t configPaxosID_, uint64_t tableID_,
 ReadBuffer& key_)
{
    type = CLIENTREQUEST_REMOVE;
    commandID = commandID_;
    configPaxosID = configPaxosID_;
    tableID = tableID_;
    key.Write(key_);
}

void ClientRequest::SequenceSet(
 uint64_t commandID_, uint64_t configPaxosID_, uint64_t tableID_,
 ReadBuffer& key_, uint64_t sequence_)
{
    type = CLIENTREQUEST_SEQUENCE_SET;
    commandID = commandID_;
    configPaxosID = configPaxosID_;
    tableID = tableID_;
    key.Write(key_);
    sequence = sequence_;
}

void ClientRequest::SequenceNext(
 uint64_t commandID_, uint64_t configPaxosID_, uint64_t tableID_,
 ReadBuffer& key_)
{
    type = CLIENTREQUEST_SEQUENCE_NEXT;
    commandID = commandID_;
    configPaxosID = configPaxosID_;
    tableID = tableID_;
    key.Write(key_);
}

void ClientRequest::ListKeys(
 uint64_t commandID_, uint64_t configPaxosID_, uint64_t tableID_, 
 ReadBuffer& startKey_, ReadBuffer& endKey_, ReadBuffer& prefix_,
 unsigned count_, bool forwardDirection_)
{
    type = CLIENTREQUEST_LIST_KEYS;
    commandID = commandID_;
    configPaxosID = configPaxosID_;
    tableID = tableID_;
    key.Write(startKey_);
    endKey.Write(endKey_);
    prefix.Write(prefix_);
    count = count_;
    forwardDirection = forwardDirection_;
}

void ClientRequest::ListKeyValues(
 uint64_t commandID_, uint64_t configPaxosID_, uint64_t tableID_,
 ReadBuffer& startKey_, ReadBuffer& endKey_, ReadBuffer& prefix_,
 unsigned count_, bool forwardDirection_)
{
    type = CLIENTREQUEST_LIST_KEYVALUES;
    commandID = commandID_;
    configPaxosID = configPaxosID_;
    tableID = tableID_;
    key.Write(startKey_);
    endKey.Write(endKey_);
    prefix.Write(prefix_);
    count = count_;
    forwardDirection = forwardDirection_;
}

void ClientRequest::Count(
 uint64_t commandID_, uint64_t configPaxosID_, uint64_t tableID_,
 ReadBuffer& startKey_, ReadBuffer& endKey_, ReadBuffer& prefix_,
 bool forwardDirection_)
{
    type = CLIENTREQUEST_COUNT;
    commandID = commandID_;
    configPaxosID = configPaxosID_;
    tableID = tableID_;
    key.Write(startKey_);
    endKey.Write(endKey_);
    prefix.Write(prefix_);
    forwardDirection = forwardDirection_;
    count = 0;
}

void ClientRequest::StartTransaction(
 uint64_t commandID_, uint64_t configPaxosID_,
 uint64_t quorumID_, ReadBuffer& majorKey_)
{
    type = CLIENTREQUEST_START_TRANSACTION;
    commandID = commandID_;
    configPaxosID = configPaxosID_;
    quorumID = quorumID_;
    key.Write(majorKey_);
}

void ClientRequest::CommitTransaction(uint64_t commandID_, uint64_t quorumID_)
{
    type = CLIENTREQUEST_COMMIT_TRANSACTION;
    commandID = commandID_;
    quorumID = quorumID_;
}

void ClientRequest::RollbackTransaction(uint64_t commandID_, uint64_t quorumID_)
{
    type = CLIENTREQUEST_ROLLBACK_TRANSACTION;
    commandID = commandID_;
    quorumID = quorumID_;
}
