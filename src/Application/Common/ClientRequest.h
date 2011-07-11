#ifndef CLIENTREQUEST_H
#define CLIENTREQUEST_H

#include "System/Containers/List.h"
#include "System/Buffers/ReadBuffer.h"
#include "ClientResponse.h"
#include "ClientSession.h"

#define CLIENTREQUEST_GET_MASTER        'm'
#define CLIENTREQUEST_GET_MASTER_HTTP   'H'
#define CLIENTREQUEST_GET_CONFIG_STATE  'A'
#define CLIENTREQUEST_CREATE_QUORUM     'Q'
#define CLIENTREQUEST_RENAME_QUORUM     'q'
#define CLIENTREQUEST_DELETE_QUORUM     'W'
#define CLIENTREQUEST_ADD_NODE          'n'
#define CLIENTREQUEST_REMOVE_NODE       'b'
#define CLIENTREQUEST_ACTIVATE_NODE     'N'
#define CLIENTREQUEST_CREATE_DATABASE   'C'
#define CLIENTREQUEST_RENAME_DATABASE   'R'
#define CLIENTREQUEST_DELETE_DATABASE   'D'
#define CLIENTREQUEST_CREATE_TABLE      'c'
#define CLIENTREQUEST_RENAME_TABLE      'r'
#define CLIENTREQUEST_DELETE_TABLE      'd'
#define CLIENTREQUEST_TRUNCATE_TABLE    't'
#define CLIENTREQUEST_FREEZE_TABLE      'F'
#define CLIENTREQUEST_UNFREEZE_TABLE    'f'
#define CLIENTREQUEST_GET               'G'
#define CLIENTREQUEST_SET               'S'
#define CLIENTREQUEST_SET_IF_NOT_EXISTS 'I'
#define CLIENTREQUEST_TEST_AND_SET      's'
#define CLIENTREQUEST_TEST_AND_DELETE   'i'
#define CLIENTREQUEST_GET_AND_SET       'g'
#define CLIENTREQUEST_ADD               'a'
#define CLIENTREQUEST_APPEND            'p'
#define CLIENTREQUEST_DELETE            'X'
#define CLIENTREQUEST_REMOVE            'x'
#define CLIENTREQUEST_LIST_KEYS         'L'
#define CLIENTREQUEST_LIST_KEYVALUES    'l'
#define CLIENTREQUEST_COUNT             'O'
#define CLIENTREQUEST_SPLIT_SHARD       'h'
#define CLIENTREQUEST_MIGRATE_SHARD     'M'
#define CLIENTREQUEST_SUBMIT            '*'
#define CLIENTREQUEST_BULK_LOADING      'B'

class ClientSession; // forward

/*
===============================================================================================

 ClientRequest

===============================================================================================
*/

class ClientRequest
{
public:
    ClientRequest();
    
    void            Init();
    void            Clear();
    void            OnComplete(bool last = true);

    bool            IsControllerRequest();
    bool            IsShardServerRequest();
    bool            IsReadRequest();
    bool            IsList();
    bool            IsActive();
    
    // Master query
    bool            GetMaster(
                     uint64_t commandID);
    bool            GetMasterHTTP(
                     uint64_t commandID);

    // Get config state: databases, tables, shards, quora
    bool            GetConfigState(
                     uint64_t commandID, uint64_t changeTimeout = 0);

    // Quorum management
    bool            CreateQuorum(
                     uint64_t commandID, ReadBuffer& name, List<uint64_t>& nodes);
    bool            RenameQuorum(
                     uint64_t commandID, uint64_t quorumID, ReadBuffer& name);
    bool            DeleteQuorum(
                     uint64_t commandID, uint64_t quorumID);
    bool            AddNode(
                     uint64_t commandID, uint64_t quorumID, uint64_t nodeID);
    bool            RemoveNode(
                     uint64_t commandID, uint64_t quorumID, uint64_t nodeID);
    bool            ActivateNode(
                     uint64_t commandID, uint64_t nodeID);
    
    // Database management
    bool            CreateDatabase(
                     uint64_t commandID, ReadBuffer& name);
    bool            RenameDatabase(
                     uint64_t commandID, uint64_t databaseID, ReadBuffer& name);
    bool            DeleteDatabase(
                     uint64_t commandID, uint64_t databaseID);
    
    // Table management
    bool            CreateTable(
                     uint64_t commandID, uint64_t databaseID, uint64_t quorumID, ReadBuffer& name);
    bool            RenameTable(
                     uint64_t commandID, uint64_t tableID, ReadBuffer& name);
    bool            DeleteTable(
                     uint64_t commandID, uint64_t tableID);
    bool            TruncateTable(
                     uint64_t commandID, uint64_t tableID);
    bool            SplitShard(
                     uint64_t commandID, uint64_t shardID, ReadBuffer& key);
    bool            FreezeTable(
                     uint64_t commandID, uint64_t tableID);
    bool            UnfreezeTable(
                     uint64_t commandID, uint64_t tableID);
    bool            MigrateShard(
                     uint64_t commandID, uint64_t shardID, uint64_t quorumID);
    
    // Data manipulations
    bool            Get(
                     uint64_t commandID, uint64_t configPaxosID_,
                     uint64_t tableID, ReadBuffer& key);
    bool            Set(
                     uint64_t commandID, uint64_t configPaxosID_,
                     uint64_t tableID, ReadBuffer& key, ReadBuffer& value);
    bool            SetIfNotExists(
                     uint64_t commandID, uint64_t configPaxosID_,
                     uint64_t tableID, ReadBuffer& key, ReadBuffer& value);
    bool            TestAndSet(
                     uint64_t commandID, uint64_t configPaxosID_,
                     uint64_t tableID, ReadBuffer& key, ReadBuffer& test, ReadBuffer& value);
    bool            TestAndDelete(
                     uint64_t commandID, uint64_t configPaxosID_,
                     uint64_t tableID, ReadBuffer& key, ReadBuffer& test);
    bool            GetAndSet(
                     uint64_t commandID, uint64_t configPaxosID_,
                     uint64_t tableID, ReadBuffer& key, ReadBuffer& value);
    bool            Add(
                     uint64_t commandID, uint64_t configPaxosID_,
                     uint64_t tableID, ReadBuffer& key, int64_t number);
    bool            Append(
                     uint64_t commandID, uint64_t configPaxosID_,
                     uint64_t tableID, ReadBuffer& key, ReadBuffer& value);
    bool            Delete(
                     uint64_t commandID, uint64_t configPaxosID_,
                     uint64_t tableID, ReadBuffer& key);    
    bool            Remove(
                     uint64_t commandID, uint64_t configPaxosID_,
                     uint64_t tableID, ReadBuffer& key);
    bool            ListKeys(
                     uint64_t commandID, uint64_t configPaxosID_,
                     uint64_t tableID, 
                     ReadBuffer& startKey, ReadBuffer& endKey, ReadBuffer& prefix,
                     unsigned count, unsigned offset);
    bool            ListKeyValues(
                     uint64_t commandID, uint64_t configPaxosID_,
                     uint64_t tableID,
                     ReadBuffer& startKey, ReadBuffer& endKey, ReadBuffer& prefix,
                     unsigned count, unsigned offset);
    bool            Count(
                     uint64_t commandID, uint64_t configPaxosID_,
                     uint64_t tableID,
                     ReadBuffer& startKey, ReadBuffer& endKey, ReadBuffer& prefix,
                     unsigned count, unsigned offset);


    bool            Submit(
                     uint64_t quorumID);
                     
    bool            BulkLoading(
                     uint64_t commandID);

    // Variables
    ClientResponse  response;
    ClientSession*  session;

    char            type;
    uint64_t        commandID;
    uint64_t        quorumID;
    uint64_t        databaseID;
    uint64_t        tableID;
    uint64_t        shardID;
    uint64_t        nodeID;
    uint64_t        paxosID;
    uint64_t        configPaxosID;
    int64_t         number;
    uint64_t        count;
    uint64_t        offset;
    Buffer          name;
    Buffer          key;
    Buffer          prefix;
    Buffer          value;
    Buffer          test;
    Buffer          endKey;
    List<uint64_t>  nodes;
    uint64_t        changeTimeout;
    uint64_t        lastChangeTime;
    bool            isBulk;
    
    ClientRequest*  prev;
    ClientRequest*  next;

};

#endif
