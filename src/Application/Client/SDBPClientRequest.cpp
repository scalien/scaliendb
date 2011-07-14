#include "SDBPClientRequest.h"

using namespace SDBPClient;

Request::Request()
{
    numTry = 0;
    numShardServers = 0;
    async = false;
    multi = false;
    parent = NULL;
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
