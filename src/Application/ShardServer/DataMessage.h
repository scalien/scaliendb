#ifndef DATAMESSAGE_H
#define DATAMESSAGE_H

#include "Framework/Messaging/Message.h"

#define DATAMESSAGE_SET         'S'
#define DATAMESSAGE_DELETE      'D'

/*
===============================================================================================

 DataMessage

===============================================================================================
*/

class DataMessage
{
public:
    // Variables
    char            type;
    uint64_t        tableID;
    ReadBuffer      key;
    ReadBuffer      value;

    // Data management
    void            Set(uint64_t tableID, ReadBuffer& key, ReadBuffer& value);
    void            Delete(uint64_t tableID, ReadBuffer& key);

    // For InList<>
    DataMessage*    prev;
    DataMessage*    next;

    // Serialization
    int             Read(ReadBuffer& buffer);
    bool            Write(Buffer& buffer);
};

#endif
