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

class DataMessage : public Message
{
public:
    // Variables
    char            type;
    uint64_t        tableID;
    Buffer          key;
    Buffer          value;

    // Data management
    void            Set(uint64_t tableID, ReadBuffer key, ReadBuffer value);
    void            Delete(uint64_t tableID, ReadBuffer key);

    // For InList<>
    DataMessage*    prev;
    DataMessage*    next;

    // Serialization
    bool            Read(ReadBuffer& buffer);
    bool            Write(Buffer& buffer);
};

#endif
