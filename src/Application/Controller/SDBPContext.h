#ifndef SDBPCONTEXT_H
#define SDBPCONTEXT_H

#include "ClientRequest.h"
#include "Command.h"

class SDBPConnection; // forward

/*
===============================================================================

 SDBPContext

===============================================================================
*/

class SDBPContext
{
public:
	virtual ~SDBPContext() {}

	/* ---------------------------------------------------------------------------------------- */
	/* SDBPContext interface:																	*/
	/*																							*/
	virtual bool	IsValidRequest(ClientRequest& request)									= 0;
	virtual bool	ProcessRequest(SDBPConnection* conn, ClientRequest& request)			= 0;	
	virtual void	OnComplete(SDBPConnection* conn, Command& command)						= 0;
	/* ---------------------------------------------------------------------------------------- */
};

#endif
