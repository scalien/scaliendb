#ifndef SDBPCONNECTION_H
#define SDBPCONNECTION_H

#include "Framework/Messaging/MessageConnection.h"
#include "Application/Common/ClientSession.h"
//#include "Application/Common/ClientRequest.h"
#include "Application/Common/ClientResponse.h"
//#include "SDBPContext.h"

class SDBPContext;
class SDBPServer;
class ClientRequest;

/*
===============================================================================

 SDBPConnection

===============================================================================
*/

class SDBPConnection : public MessageConnection, public ClientSession
{
public:
	SDBPConnection();
	
	void				Init(SDBPServer* server);
	void				SetContext(SDBPContext* context);

	/* ---------------------------------------------------------------------------------------- */
	/* MessageConnection interface:																*/
	/*																							*/
	/* OnMessage() returns whether the connection was closed and deleted						*/
	bool				OnMessage(ReadBuffer& msg);
	/* Must overrise OnWrite(), to handle closeAfterSend()										*/
	void				OnWrite();
	/* Must override OnClose() to prevent the default behaviour, which is to call Close(),		*/
	/* in case numPendingOps > 0																*/
	void				OnClose();
	/* ---------------------------------------------------------------------------------------- */

	/* ---------------------------------------------------------------------------------------- */
	/* ClientConnection interface																*/
	/*																							*/
	virtual void		OnComplete(Message* message);
	virtual bool		IsActive();
	/* ---------------------------------------------------------------------------------------- */
	
	void				SendResponse(ClientResponse& response, bool closeAfterSend = false);

private:
	SDBPServer*			server;
	SDBPContext*		context;
	unsigned			numPendingOps;
	bool				closeAfterSend;
	Buffer				writeBuffer;
};

#endif
