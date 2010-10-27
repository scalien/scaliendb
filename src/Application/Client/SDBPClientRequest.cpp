#include "SDBPClientRequest.h"

using namespace SDBPClient;

Request::Request()
{
    prev = next = this;
}
