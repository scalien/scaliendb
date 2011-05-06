#ifndef CLIENTRESPONSE_H
#define CLIENTRESPONSE_H

#include "System/Buffers/ReadBuffer.h"
#include "Application/ConfigState/ConfigState.h"

#define CLIENTRESPONSE_OK               'O'
#define CLIENTRESPONSE_NUMBER           'n'
#define CLIENTRESPONSE_VALUE            'V'
#define CLIENTRESPONSE_LIST_KEYS        'L'
#define CLIENTRESPONSE_LIST_KEYVALUES   'l'
#define CLIENTRESPONSE_CONFIG_STATE     'C'
#define CLIENTRESPONSE_NOSERVICE        'S'
#define CLIENTRESPONSE_FAILED           'F'
#define CLIENTRESPONSE_NORESPONSE       ' '
#define CLIENTRESPONSE_HELLO            '_'
#define CLIENTRESPONSE_NEXT             'N'

#define CLIENTRESPONSE_OPT_PAXOSID              'P'
#define CLIENTRESPONSE_OPT_VALUE_CHANGED        'v'

// this is needed on Visual C++ which cannot handle C99 type dynamic stack arrays
#ifdef PLATFORM_WINDOWS
#define CLIENTRESPONSE_MAX_LIST_ITEMS			65536
#define CLIENTRESPONSE_NUMKEYS(numKeys)			CLIENTRESPONSE_MAX_LIST_ITEMS
#define CLIENTRESPONSE_ASSERT_NUMKEYS(numKeys)	ASSERT(numKeys <= CLIENTRESPONSE_MAX_LIST_ITEMS);
#else
#define CLIENTRESPONSE_NUMKEYS(numKeys)			numKeys
#define CLIENTRESPONSE_ASSERT_NUMKEYS(numKeys)
#endif

class ClientRequest; // forward

/*
===============================================================================================

 ClientResponse

===============================================================================================
*/

class ClientResponse
{
public:
    ClientRequest*  request;
    
    /* Variables */
    char            type;
    uint64_t        number;
    uint64_t        offset;
    uint64_t        commandID;
    uint64_t        paxosID;
    ReadBuffer      value;
    ReadBuffer      endKey;
    unsigned        numKeys;
    ReadBuffer*     keys;
    ReadBuffer*     values;
    Buffer*         valueBuffer;
    bool            isValueChanged;
    ConfigState     configState;

    ClientResponse();
    ~ClientResponse();
    
    void            Init();
    void            Clear();
    void            CopyValue();
    void            CopyKeys();
    void            CopyKeyValues();
    void            Transfer(ClientResponse& other);
            
    /* Responses */
    bool            OK();
    bool            Number(uint64_t number);
    bool            Value(ReadBuffer& value);
    bool            ListKeys(unsigned numListKeys, ReadBuffer* keys);
    bool            ListKeyValues(unsigned numListKeys, ReadBuffer* keys, ReadBuffer* values);
    bool            ConfigStateResponse(ConfigState& configState);
    bool            NoService();
    bool            Failed();
    bool            NoResponse();
    bool            Hello();
    bool            Next(ReadBuffer& nextShardKey, ReadBuffer& endKey, uint64_t count, uint64_t offset);

    void            SetValueChanged(bool isValueChanged);
};

#endif
