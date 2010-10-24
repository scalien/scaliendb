#ifndef CLIENTREQUEST_H
#define CLIENTREQUEST_H

#include "System/Containers/ArrayList.h"
#include "System/Buffers/ReadBuffer.h"
#include "ClientResponse.h"
#include "ClientSession.h"

#define CLIENTREQUEST_GET_MASTER        'm'
#define CLIENTREQUEST_GET_CONFIG_STATE  'A'
#define CLIENTREQUEST_CREATE_QUORUM     'Q'
#define CLIENTREQUEST_CREATE_DATABASE   'C'
#define CLIENTREQUEST_RENAME_DATABASE   'R'
#define CLIENTREQUEST_DELETE_DATABASE   'D'
#define CLIENTREQUEST_CREATE_TABLE      'c'
#define CLIENTREQUEST_RENAME_TABLE      'r'
#define CLIENTREQUEST_DELETE_TABLE      'd'
#define CLIENTREQUEST_GET               'G'
#define CLIENTREQUEST_SET               'S'
#define CLIENTREQUEST_SET_IF_NOT_EXISTS 'I'
#define CLIENTREQUEST_TEST_AND_SET      's'
#define CLIENTREQUEST_ADD               'a'
#define CLIENTREQUEST_DELETE            'X'
#define CLIENTREQUEST_REMOVE            'x'

class ClientSession; // forward

/*
===============================================================================================

 ClientRequest

===============================================================================================
*/

class ClientRequest
{
public:
    typedef ArrayList<uint64_t, CONFIG_MAX_NODES> NodeList;
    
    ClientRequest();
    
    void OnComplete(bool last = true);

    bool            IsControllerRequest();
    bool            IsShardServerRequest();
    bool            IsSafeRequest();
    
    // Master query
    bool            GetMaster(
                     uint64_t commandID);

    // Get config state: databases, tables, shards, quora
    bool            GetConfigState(
                     uint64_t commandID);

    // Quorum management
    bool            CreateQuorum(
                     uint64_t commandID, NodeList& nodes);  
                 
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
                     uint64_t commandID, uint64_t databaseID, uint64_t tableID, ReadBuffer& name);
    bool            DeleteTable(
                     uint64_t commandID, uint64_t databaseID, uint64_t tableID);
    
    // Data manipulations
    bool            Get(
                     uint64_t commandID, uint64_t databaseID,
                     uint64_t tableID, ReadBuffer& key);
    bool            Set(
                     uint64_t commandID, uint64_t databaseID,
                     uint64_t tableID, ReadBuffer& key, ReadBuffer& value);
    bool            SetIfNotExists(
                     uint64_t commandID, uint64_t databaseID,
                     uint64_t tableID, ReadBuffer& key, ReadBuffer& value);
    bool            TestAndSet(
                     uint64_t commandID, uint64_t databaseID,
                     uint64_t tableID, ReadBuffer& key, ReadBuffer& test, ReadBuffer& value);
    bool            Add(
                     uint64_t commandID, uint64_t databaseID,
                     uint64_t tableID, ReadBuffer& key, int64_t number);
    bool            Delete(
                     uint64_t commandID, uint64_t databaseID,
                     uint64_t tableID, ReadBuffer& key);    
    bool            Remove(
                     uint64_t commandID, uint64_t databaseID,
                     uint64_t tableID, ReadBuffer& key);    

    // Variables
    ClientResponse  response;
    ClientSession*  session;

    char            type;
    uint64_t        commandID;
    uint64_t        quorumID;
    uint64_t        databaseID;
    uint64_t        tableID;
    uint64_t        shardID;
    int64_t         number;
    Buffer          name;
    Buffer          key;
    Buffer          value;
    Buffer          test;
    NodeList        nodes;
    
    ClientRequest*  prev;
    ClientRequest*  next;

};

#endif
