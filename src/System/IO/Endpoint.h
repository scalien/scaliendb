#ifndef ENDPOINT_H
#define ENDPOINT_H

#include "System/Platform.h"
#include "System/Log.h"

#define ENDPOINT_ANY_ADDRESS	0
#define ENDPOINT_STRING_SIZE	32
#define ENDPOINT_SOCKADDR_SIZE	16

class Endpoint
{
public:
	Endpoint();
	
	bool			operator==(const Endpoint &other) const;
	bool			operator!=(const Endpoint &other) const;
	
	bool			Set(const char* ip, int port, bool resolve = false);
	bool			Set(const char* ip_port, bool resolve = false);

	bool			SetPort(int port);
	int				GetPort();
	
	uint32_t		GetAddress();
	
	const char*		ToString();
	const char*		ToString(char s[ENDPOINT_STRING_SIZE]);

	char*			GetSockAddr() { return saBuffer; }

private:
	char			buffer[ENDPOINT_STRING_SIZE];
	char			saBuffer[ENDPOINT_SOCKADDR_SIZE];
};

#endif
