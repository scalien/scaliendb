#include "MessageReader.h"
#include "System/Common.h"

/*
===============================================================================

 MessageReaderConnection

===============================================================================
*/

void MessageReaderConnection::SetServer(MessageReader* reader_)
{
	reader = reader_;
}
	
void MessageReaderConnection::OnMessage()
{
	reader->OnMessage(GetMessage());
}
	
void MessageReaderConnection::OnClose()
{
	Log_Trace();
	
	Close();
	
	reader->OnConnectionClose(this);

	delete this;
}

/*
===============================================================================

 MessageReader

===============================================================================
*/

bool MessageReader::Init(int port)
{
	running = true;
	return TCPServer<MessageReader, MessageReaderConnection>::Init(port);
}

void MessageReader::SetOnRead(const Callable& onRead_)
{
	onRead = onRead_;
}

void MessageReader::OnMessage(ReadBuffer msg_)
{
	msg = msg_;
	Call(onRead);
}

ReadBuffer MessageReader::GetMessage()
{
	return msg;
}

bool MessageReader::IsActive()
{
	return running;
}

void MessageReader::Stop()
{
	Log_Trace();
	
	MessageReaderConnection** it;

	running = false;
	
	for (it = conns.Head(); it != NULL; it = conns.Next(it))
	{
		(*it)->Stop();
		if (running)
			break; // user called Continue(), stop looping
	}
}

void MessageReader::Continue()
{
	Log_Trace();

	MessageReaderConnection** it;
	
	running = true;
	
	for (it = conns.Head(); it != NULL; it = conns.Next(it))
	{
		(*it)->Continue();
		if (!running)
			break; // user called Stop(), stop looping
	}
}

void MessageReader::InitConn(MessageReaderConnection* conn)
{
	conn->SetServer(this);
}

void MessageReader::OnConnectionClose(MessageReaderConnection* conn)
{
	assert(conns.Remove(conn));
}

