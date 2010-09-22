#ifndef CLIENTSESSION_H
#define CLIENTSESSION_H

class ClientRequest; // forward

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
	
	virtual void	OnComplete(ClientRequest* request, bool last)		= 0;
	virtual bool	IsActive()											= 0;
};

#endif
