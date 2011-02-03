#include "SDBPResponseMessage.h"
#include "Application/Common/ClientRequest.h"

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
            break;
        case CLIENTRESPONSE_NUMBER:
            read = buffer.Readf("%c:%U:%U",
             &response->type, &response->commandID, &response->number);
            break;
        case CLIENTRESPONSE_VALUE:
            read = buffer.Readf("%c:%U:%#R",
             &response->type, &response->commandID, &response->value);
            break;
        case CLIENTRESPONSE_CONFIG_STATE:
            read = buffer.Readf("%c:%U:",
             &response->type, &response->commandID);
            if (read <= 0)
                return false;
            buffer.Advance(read);
            Log_Debug("%B", &buffer);
            if (!response->configState.Read(buffer, true))
            {
                response->configState.Init();
                return false;
            }
            return true;
        case CLIENTRESPONSE_NOSERVICE:
            read = buffer.Readf("%c:%U",
             &response->type, &response->commandID);
            break;
        case CLIENTRESPONSE_FAILED:
            read = buffer.Readf("%c:%U",
             &response->type, &response->commandID);
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
            return true;
        case CLIENTRESPONSE_NUMBER:
            buffer.Writef("%c:%U:%U",
             response->type, response->request->commandID, response->number);
            return true;
        case CLIENTRESPONSE_VALUE:
            buffer.Writef("%c:%U:%#R",
             response->type, response->request->commandID, &response->value);
            return true;
        case CLIENTRESPONSE_CONFIG_STATE:
            buffer.Writef("%c:%U:",
             response->type, response->request->commandID);
            return response->configState.Write(buffer, true);
        case CLIENTRESPONSE_NOSERVICE:
            buffer.Writef("%c:%U",
             response->type, response->request->commandID);
            return true;
        case CLIENTRESPONSE_FAILED:
            buffer.Writef("%c:%U",
             response->type, response->request->commandID);
            return true;
        default:
            return false;
    }
}
