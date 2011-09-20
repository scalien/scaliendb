#include "SDBPClientRequest.h"

using namespace SDBPClient;

Request::Request()
{
    numTry = 0;
    numShardServers = 0;
    skip = false;
    client = NULL;
    requestTime = 0;
    responseTime = 0;
    userCount = 0;
}

Request::~Request()
{
    ClientResponse**    itResponse;

    FOREACH_FIRST (itResponse, responses)
    {
        delete *itResponse;
        responses.Remove(itResponse);
    }
}

unsigned Request::GetNumResponses()
{
    ClientResponse**    itResponse;
    unsigned            numResponses;
    
    numResponses = 0;
    FOREACH (itResponse, responses)
        numResponses += (*itResponse)->numKeys;
    
    return numResponses;
}
