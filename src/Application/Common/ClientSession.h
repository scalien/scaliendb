#ifndef CLIENTSESSION_H
#define CLIENTSESSION_H

#include "Framework/Messaging/Message.h"

/*
===============================================================================================

 ClientSession
 
 An ADT that represents connections that takes ScalienDB commands.
 Currently: HTTP, SDBP.

===============================================================================================
*/

class ClientSession
{
public:
	virtual ~ClientSession() {}
	
	virtual void	OnComplete(Message* message, bool status)	= 0;
	virtual bool	IsActive()									= 0;
};

#endif
