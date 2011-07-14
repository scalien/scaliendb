#ifndef CLIENTRESPONSE_H
#define CLIENTRESPONSE_H

#include "System/PointerGuard.h"
#include "System/Buffers/ReadBuffer.h"
#include "Application/ConfigState/ConfigState.h"

#define CLIENTRESPONSE_OK               'O'
#define CLIENTRESPONSE_SNUMBER          's'
#define CLIENTRESPONSE_NUMBER           'n'
#define CLIENTRESPONSE_VALUE            'V'
#define CLIENTRESPONSE_LIST_KEYS        'L'
#define CLIENTRESPONSE_LIST_KEYVALUES   'l'
#define CLIENTRESPONSE_CONFIG_STATE     'C'
#define CLIENTRESPONSE_NOSERVICE        'S'
#define CLIENTRESPONSE_BADSCHEMA        'B'
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
    typedef PointerGuard<ConfigState>   ConfigStateGuard;
public:
    ClientRequest*  request;
    
    /* Variables */
    char            type;
    int64_t         snumber;
    uint64_t        number;
    uint64_t        commandID;
    uint64_t        paxosID;
    ReadBuffer      value;
    ReadBuffer      endKey;
    ReadBuffer      prefix;
    unsigned        numKeys;
    ReadBuffer*     keys;
    ReadBuffer*     values;
    Buffer*         valueBuffer;
    bool            isConditionalSuccess;
    ConfigStateGuard configState;

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
    bool            SignedNumber(int64_t snumber);
    bool            Number(uint64_t number);
    bool            Value(ReadBuffer& value);
    bool            ListKeys(unsigned numListKeys, ReadBuffer* keys);
    bool            ListKeyValues(unsigned numListKeys, ReadBuffer* keys, ReadBuffer* values);
    bool            ConfigStateResponse(ConfigState& configState);
    bool            NoService();
    bool            BadSchema();
    bool            Failed();
    bool            NoResponse();
    bool            Hello();
    bool            Next(ReadBuffer& nextShardKey, ReadBuffer& endKey, ReadBuffer& prefix,
                     uint64_t count);

    void            SetConditionalSuccess(bool isConditionalSuccess);
};

#endif
