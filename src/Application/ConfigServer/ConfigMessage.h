#ifndef CONFIGMESSAGE_H
#define CONFIGMESSAGE_H

#include "System/Platform.h"
#include "System/IO/Endpoint.h"
#include "System/Containers/ArrayList.h"
#include "Framework/Messaging/Message.h"

#define CONFIGMESSAGE_MAX_NODES                 7

#define CONFIGMESSAGE_REGISTER_SHARDSERVER      'S'
#define CONFIGMESSAGE_CREATE_QUORUM             'Q'
#define CONFIGMESSAGE_INCREASE_QUORUM           'P'
#define CONFIGMESSAGE_DECREASE_QUORUM           'M'
#define CONFIGMESSAGE_ACTIVATE_SHARDSERVER      'p'
#define CONFIGMESSAGE_DEACTIVATE_SHARDSERVER    'm'

#define CONFIGMESSAGE_CREATE_DATABASE           'C'
#define CONFIGMESSAGE_RENAME_DATABASE           'R'
#define CONFIGMESSAGE_DELETE_DATABASE           'D'

#define CONFIGMESSAGE_CREATE_TABLE              'c'
#define CONFIGMESSAGE_RENAME_TABLE              'r'
#define CONFIGMESSAGE_DELETE_TABLE              'd'

/*
===============================================================================================

 ConfigMessage

===============================================================================================
*/

class ConfigMessage : public Message
{
public:
    typedef ArrayList<uint64_t, CONFIGMESSAGE_MAX_NODES> NodeList;

    ConfigMessage() { prev = next = this; }

    // Variables
    bool            fromClient;

    char            type;
    uint64_t        nodeID;
    uint64_t        quorumID;
    uint64_t        databaseID;
    uint64_t        tableID;
    uint64_t        shardID;
    ReadBuffer      name;
    Endpoint        endpoint;
    NodeList        nodes;    
    
    // For InList<>
    ConfigMessage*  prev;
    ConfigMessage*  next;

    // Cluster management
    bool            RegisterShardServer(
                     uint64_t nodeID, Endpoint& endpoint);
    bool            CreateQuorum(
                     uint64_t quorumID, NodeList& nodes);
//    bool            IncreaseQuorum(
//                     uint64_t quorumID, uint64_t nodeID);
//    bool            DecreaseQuorum(
//                     uint64_t quorumID, uint64_t nodeID);
    bool            ActivateShardServer(
                     uint64_t quorumID, uint64_t nodeID);
    bool            DeactivateShardServer(
                     uint64_t quorumID, uint64_t nodeID);

    // Database management
    bool            CreateDatabase(
                     uint64_t databaseID, ReadBuffer& name);
    bool            RenameDatabase(
                     uint64_t databaseID, ReadBuffer& name);
    bool            DeleteDatabase(
                     uint64_t databaseID);

    // Table management
    bool            CreateTable(
                     uint64_t databaseID, uint64_t tableID, uint64_t shardID,
                     uint64_t quorumID, ReadBuffer& name);
    bool            RenameTable(
                     uint64_t databaseID, uint64_t tableID, ReadBuffer& name);
    bool            DeleteTable(
                     uint64_t databaseID, uint64_t tableID);
    
    // Serialization
    bool            Read(ReadBuffer& buffer);
    bool            Write(Buffer& buffer);
};

#endif
