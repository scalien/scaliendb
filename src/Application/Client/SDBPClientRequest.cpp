#include "SDBPClientRequest.h"

using namespace SDBPClient;

Request::Request()
{
    numTry = 0;
    numQuorums = 1;
    numShardServers = 0;
    async = false;
}
