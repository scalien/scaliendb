#ifndef CLIENTREQUEST_H
#define CLIENTREQUEST_H

#include "System/Containers/List.h"
#include "System/Buffers/ReadBuffer.h"
#include "ClientResponse.h"
#include "ClientSession.h"

#define CLIENTREQUEST_GET_MASTER        'm'
#define CLIENTREQUEST_GET_CONFIG_STATE  'A'
#define CLIENTREQUEST_CREATE_QUORUM     'Q'
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
#define CLIENTREQUEST_GET               'G'
#define CLIENTREQUEST_SET               'S'
#define CLIENTREQUEST_SET_IF_NOT_EXISTS 'I'
#define CLIENTREQUEST_TEST_AND_SET      's'
#define CLIENTREQUEST_GET_AND_SET       'g'
#define CLIENTREQUEST_ADD               'a'
#define CLIENTREQUEST_APPEND            'p'
#define CLIENTREQUEST_DELETE            'X'
#define CLIENTREQUEST_REMOVE            'x'
#define CLIENTREQUEST_LIST_KEYS         'L'
#define CLIENTREQUEST_LIST_KEYVALUES    'l'
#define CLIENTREQUEST_SPLIT_SHARD       'h'
#define CLIENTREQUEST_SUBMIT            '*'

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
    void            OnComplete(bool last = true);

    bool            IsControllerRequest();
    bool            IsShardServerRequest();
    bool            IsSafeRequest();
    bool            IsList();
    
    // Master query
    bool            GetMaster(
                     uint64_t commandID);

    // Get config state: databases, tables, shards, quora
    bool            GetConfigState(
                     uint64_t commandID, uint64_t changeTimeout = 0);

    // Quorum management
    bool            CreateQuorum(
                     uint64_t commandID, List<uint64_t>& nodes);
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
//    bool            SplitShard(
//                     uint64_t commandID, uint64_t shardID, ReadBuffer& key);
    
    // Data manipulations
    bool            Get(
                     uint64_t commandID,
                     uint64_t tableID, ReadBuffer& key);
    bool            Set(
                     uint64_t commandID,
                     uint64_t tableID, ReadBuffer& key, ReadBuffer& value);
    bool            SetIfNotExists(
                     uint64_t commandID,
                     uint64_t tableID, ReadBuffer& key, ReadBuffer& value);
    bool            TestAndSet(
                     uint64_t commandID,
                     uint64_t tableID, ReadBuffer& key, ReadBuffer& test, ReadBuffer& value);
    bool            GetAndSet(
                     uint64_t commandID,
                     uint64_t tableID, ReadBuffer& key, ReadBuffer& value);
    bool            Add(
                     uint64_t commandID,
                     uint64_t tableID, ReadBuffer& key, int64_t number);
    bool            Append(
                     uint64_t commandID,
                     uint64_t tableID, ReadBuffer& key, ReadBuffer& value);
    bool            Delete(
                     uint64_t commandID,
                     uint64_t tableID, ReadBuffer& key);    
    bool            Remove(
                     uint64_t commandID,
                     uint64_t tableID, ReadBuffer& key);
    bool            ListKeys(
                     uint64_t commandID,
                     uint64_t tableID, ReadBuffer& startKey, unsigned count, unsigned offset);
    bool            ListKeyValues(
                     uint64_t commandID,
                     uint64_t tableID, ReadBuffer& startKey, unsigned count, unsigned offset);

    bool            Submit(
                     uint64_t quorumID);

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
    int64_t         number;
    unsigned        count;
    unsigned        offset;
    Buffer          name;
    Buffer          key;
    Buffer          value;
    Buffer          test;
    List<uint64_t>  nodes;
    uint64_t        changeTimeout;
    
    ClientRequest*  prev;
    ClientRequest*  next;

};

#endif
