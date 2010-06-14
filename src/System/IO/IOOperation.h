#ifndef IOOPERATION_H
#define IOOPERATION_H

#ifdef PLATFORM_DARWIN
#include <sys/types.h>
#endif

#include "System/Events/Callable.h"
#include "Buffers.h"
#include "Endpoint.h"
#include "FD.h"

/* all the cursor-type members of IOOperations are absolute cursors */

#define IO_READ_ANY -1

struct IOOperation
{

	IOOperation()
	{
		fd = INVALID_FD;
		active = false;
		pending = false;
		offset = 0;
		onComplete = NULL; 
		onClose = NULL; 
	}

	enum Type		{ UDP_WRITE, UDP_READ, TCP_WRITE, TCP_READ };
	
	ByteWrap		data;
	
	Type			type;
	FD				fd;
	int				offset;
	
	bool			active;
	bool			pending;

	Callable*		onComplete;
	Callable*		onClose;
};

struct UDPWrite : public IOOperation
{
	UDPWrite()
	: IOOperation()
	{
		type = UDP_WRITE;
	}

	Endpoint		endpoint;
};

struct UDPRead : public IOOperation
{
public:
	UDPRead()
	: IOOperation()
	{
		type = UDP_READ;
	}

public:
	Endpoint		endpoint;
};

struct TCPWrite : public IOOperation
{
	TCPWrite()
	: IOOperation()
	{
		type = TCP_WRITE;
		transferred = 0;
	}

	unsigned	transferred;		/*	the IO subsystem has given the first
										'transferred' bytes to the kernel */
};

struct TCPRead : public IOOperation
{
	TCPRead()
	: IOOperation()
	{
		type = TCP_READ;
		listening = false;
		requested = 0;
	}

	bool			listening;		/*	it's a listener */
	int				requested;		/*	application is requesting the buffer
										to be filled exactly with a total of
										'requested' bytes; if the application
										has no idea how much to expect, it sets
										'requested' to IO_READ_ANY; 'callback'
										is called when request bytes have been
										read into the buffer; useful for cases
										when header includes length */
};

#endif

