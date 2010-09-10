#include "Application/Common/ClientRequest.h"

int main(int argc, char** argv)
{
	ClientRequest request;
	ReadBuffer buffer;
	
	request.Read(buffer);
}
