#include "SDBPClientRequest.h"

using namespace SDBPClient;

Request::Request()
{
    numTry = 0;
    numQuorums = 1;
}