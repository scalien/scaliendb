#ifndef MESSAGEREADER_H
#define	MESSAGEREADER_H

#include "Framework/TCP/TCPServer.h"
#include "MessageConnection.h"

class MessageReader; // forward

/*
===============================================================================

 MessageReaderConnection

===============================================================================
*/

class MessageReaderConnection : public MessageConnection
{
public:
	void				SetServer(MessageReader* reader);
		
	void				OnMessage();
	void				OnClose();

private:
	MessageReader*		reader;
};

/*
===============================================================================

 MessageReader

===============================================================================
*/

class MessageReader : public TCPServer<MessageReader, MessageReaderConnection>
{
	friend class MessageReaderConnection;
	typedef List<MessageReaderConnection*> ConnList;

public:
	bool				Init(int port);

	void				SetOnRead(const Callable& onRead);
	ReadBuffer			GetMessage();
	void				Stop();
	void				Continue();
	bool				IsActive();
	void				InitConn(MessageReaderConnection* conn);
	void				OnConnectionClose(MessageReaderConnection* conn);

private:
	void				OnMessage(ReadBuffer buffer);
	
	Callable			onRead;
	ReadBuffer			msg;
	ConnList			conns;
	bool				running;
};

#endif
