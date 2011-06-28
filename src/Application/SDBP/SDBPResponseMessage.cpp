#include "SDBPResponseMessage.h"
#include "Application/Common/ClientRequest.h"
#include "Version.h"

bool SDBPResponseMessage::Read(ReadBuffer& buffer)
{
    int             read;
        
    if (buffer.GetLength() < 1)
        return false;
    
    switch (buffer.GetCharAt(0))
    {
        case CLIENTRESPONSE_OK:
            read = buffer.Readf("%c:%U",
             &response->type, &response->commandID);
            read = ReadOptionalParts(buffer, read);
            break;
        case CLIENTRESPONSE_NUMBER:
            read = buffer.Readf("%c:%U:%U",
             &response->type, &response->commandID, &response->number);
            read = ReadOptionalParts(buffer, read);
            break;
        case CLIENTRESPONSE_SNUMBER:
            read = buffer.Readf("%c:%U:%I",
             &response->type, &response->commandID, &response->snumber);
            read = ReadOptionalParts(buffer, read);
            break;
        case CLIENTRESPONSE_VALUE:
            read = buffer.Readf("%c:%U:%#R",
             &response->type, &response->commandID, &response->value);
            read = ReadOptionalParts(buffer, read);
            break;
        case CLIENTRESPONSE_LIST_KEYS:
            read = buffer.Readf("%c:%U:%u",
             &response->type, &response->commandID, &response->numKeys);
            buffer.Advance(read);

            if (response->numKeys != 0)
            {
                ReadBuffer*		keys;
				keys = new ReadBuffer[response->numKeys];
                for (unsigned i = 0; i < response->numKeys; i++)
                {
                    read = buffer.Readf(":%#R", &keys[i]);
                    buffer.Advance(read);
                }
                response->ListKeys(response->numKeys, keys);
				delete[] keys;
            }
            return true;
        case CLIENTRESPONSE_LIST_KEYVALUES:
            read = buffer.Readf("%c:%U:%u",
             &response->type, &response->commandID, &response->numKeys);
            buffer.Advance(read);
            
            if (response->numKeys != 0)
            {
				ReadBuffer*		keys;
                ReadBuffer*		values;
				keys = new ReadBuffer[response->numKeys];
				values = new ReadBuffer[response->numKeys];
                for (unsigned i = 0; i < response->numKeys; i++)
                {
                    read = buffer.Readf(":%#R:%#R", &keys[i], &values[i]);
                    buffer.Advance(read);
                }
                response->ListKeyValues(response->numKeys, keys, values);
				delete[] keys;
				delete[] values;
            }
            return true;
        case CLIENTRESPONSE_CONFIG_STATE:
            read = buffer.Readf("%c:%U:",
             &response->type, &response->commandID);
            if (read <= 0)
                return false;
            buffer.Advance(read);
            if (!response->configState.Read(buffer, true))
            {
                response->configState.Init();
                return false;
            }
            return true;
        case CLIENTRESPONSE_NOSERVICE:
        case CLIENTRESPONSE_BADSCHEMA:
        case CLIENTRESPONSE_FAILED:
            read = buffer.Readf("%c:%U",
             &response->type, &response->commandID);
            break;
        case CLIENTRESPONSE_HELLO:
            read = buffer.Readf("%c:%U:%U:%#R",
             &response->type, &response->commandID,
             &response->number, &response->value);
            return true;
        case CLIENTRESPONSE_NEXT:
            read = buffer.Readf("%c:%U:%U:%U:%#R:%#R",
             &response->type, &response->commandID, 
             &response->number, &response->offset, &response->value, &response->endKey);
            break;
        default:
            return false;
    }
    
    return (read == (signed)buffer.GetLength() ? true : false);
}

bool SDBPResponseMessage::Write(Buffer& buffer)
{
    switch (response->type)
    {
        case CLIENTRESPONSE_OK:
            buffer.Writef("%c:%U",
             response->type, response->request->commandID);
            WriteOptionalParts(buffer);
            return true;
        case CLIENTRESPONSE_NUMBER:
            buffer.Writef("%c:%U:%U",
             response->type, response->request->commandID, response->number);
            WriteOptionalParts(buffer);
            return true;
        case CLIENTRESPONSE_SNUMBER:
            buffer.Writef("%c:%U:%I",
             response->type, response->request->commandID, response->snumber);
            WriteOptionalParts(buffer);
            return true;
        case CLIENTRESPONSE_VALUE:
            buffer.Writef("%c:%U:%#R",
             response->type, response->request->commandID, &response->value);
            WriteOptionalParts(buffer);
            return true;
        case CLIENTRESPONSE_LIST_KEYS:
            buffer.Writef("%c:%U:%u",
             response->type, response->request->commandID, response->numKeys);
            for (unsigned i = 0; i < response->numKeys; i++)
                buffer.Appendf(":%#R", &response->keys[i]);
            return true;
        case CLIENTRESPONSE_LIST_KEYVALUES:
            buffer.Writef("%c:%U:%u",
             response->type, response->request->commandID, response->numKeys);
            for (unsigned i = 0; i < response->numKeys; i++)
                buffer.Appendf(":%#R:%#R", &response->keys[i], &response->values[i]);
            return true;            
        case CLIENTRESPONSE_CONFIG_STATE:
            buffer.Writef("%c:%U:",
             response->type, response->request->commandID);
            return response->configState.Write(buffer, true);
        case CLIENTRESPONSE_NOSERVICE:
        case CLIENTRESPONSE_BADSCHEMA:
        case CLIENTRESPONSE_FAILED:
            buffer.Writef("%c:%U",
             response->type, response->request->commandID);
            return true;
        case CLIENTRESPONSE_HELLO:
            {
                uint64_t    clientVersion = 1;
                uint64_t    commandID = 0;
                Buffer      msg;

                msg.Writef("SDBP client protocol, server version v%s\n", VERSION_STRING);
                buffer.Writef("%c:%U:%U:%#B",
                 response->type, commandID,
                 clientVersion, &msg);
            }
            return true;
        case CLIENTRESPONSE_NEXT:
            Log_Trace("Next");
            buffer.Writef("%c:%U:%U:%U:%#R:%#R",
             response->type, response->request->commandID, 
             response->number, response->offset, &response->value, &response->endKey);
            return true;        
        default:
            return false;
    }
}

int SDBPResponseMessage::ReadOptionalParts(ReadBuffer buffer, int offset)
{
    unsigned    length;
    unsigned    pos;
    char        opt;
    char        type;
    int         read;
    ReadBuffer  tmpBuffer;
    uint64_t    tmpNumber;
    
    length = buffer.GetLength();
    pos = offset;
    buffer.Advance(offset);
    
    while (length > pos)
    {
        // each optional command structured as follows:
        // <1 byte COLON><1 byte OPT_COMMAND><1 byte type prefix><DATA>
        // where the length of DATA depends on the type prefix
        if (buffer.GetLength() < 2)
            break;
        read = buffer.Readf(":%c", &opt);
        if (read != 2)
            return read;

        read = 0;
        switch (opt)
        {
            case CLIENTRESPONSE_OPT_PAXOSID:
                read = buffer.Readf(":%cU%U", &opt, &response->paxosID);
                break;
            case CLIENTRESPONSE_OPT_VALUE_CHANGED:
                read = buffer.Readf(":%cb%b", &opt, &response->isConditionalSuccess);
                break;
            default:
                // read any other message based on the type prefix
                buffer.Advance(2);
                read = buffer.Readf("%c", &type);
                if (read < 0)
                    return read;
                switch (type) {
                    case 'U':   // numeric
                        read = buffer.Readf("%U", &tmpNumber);
                        break;
                    case 'b':   // bool
                        read = 1;
                        break;
                    case 'B':   // buffer
                        read = buffer.Readf("%#R", &tmpBuffer);
                        break;
                    default:
                        return -1;
                }
                break;
        }
        if (read < 0)
            return read;
        
        buffer.Advance(read);
        pos += read;
    }
    
    return pos;
}

void SDBPResponseMessage::WriteOptionalParts(Buffer& buffer)
{
    if (response->paxosID > 0)
        buffer.Appendf(":%cU%U", CLIENTRESPONSE_OPT_PAXOSID, response->paxosID);
    if (response->isConditionalSuccess)
        buffer.Appendf(":%cb%b", CLIENTRESPONSE_OPT_VALUE_CHANGED, response->isConditionalSuccess);
}
