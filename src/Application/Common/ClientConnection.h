#ifndef CLIENTCONNECTION_H
#define CLIENTCONNECTION_H

#include "Command.h"

/*
===============================================================================

 ClientConnection
 
 An ADT that represents connections that takes ScalienDB commands.
 Currently: HTTP, SDBP.

===============================================================================
*/

class ClientConnection
{
public:
	virtual ~ClientConnection() {}
	
	virtual void	OnComplete(Command* command)	= 0;
	virtual bool	IsActive()						= 0;
};

#endif
