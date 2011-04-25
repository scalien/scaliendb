#include "SDBPClientRequest.h"

using namespace SDBPClient;

Request::Request()
{
    numTry = 0;
    numShardServers = 0;
    numBulkResponses = 0;
    async = false;
    multi = false;
    parent = NULL;
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
