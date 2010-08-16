#include "MessageConnection.h"
#include "MessageTransport.h"

MessageConnection::MessageConnection()
{
	resumeRead.SetCallable(MFUNC(MessageConnection, OnResumeRead));
	resumeRead.SetDelay(1);
}

void MessageConnection::InitConnected(bool startRead)
{
	TCPConnection::InitConnected(startRead);
	
	Log_Trace();

	progress = INCOMING;
	nodeID = 0;
}

void MessageConnection::SetTransport(MessageTransport* transport_)
{
	Log_Trace();
	
	transport = transport_;
}

void MessageConnection::SetNodeID(uint64_t nodeID_)
{
	nodeID = nodeID_;
}

void MessageConnection::SetEndpoint(Endpoint& endpoint_)
{
	endpoint = endpoint_;
}

uint64_t MessageConnection::GetNodeID()
{
	return nodeID;
}

MessageConnection::Progress MessageConnection::GetProgress()
{
	return progress;
}

void MessageConnection::Write(Buffer& msg)
{
	Buffer* buffer;
	
	buffer = writer->AcquireBuffer();
	buffer->Writef("%#B", &msg);
	writer->Write(buffer);	
	writer->Flush();
}

void MessageConnection::WritePriority(Buffer& msg)
{
	Buffer* buffer;
	
	buffer = writer->AcquireBuffer();
	buffer->Writef("%#B", &msg);
	writer->WritePriority(buffer);	
	writer->Flush();
}

void MessageConnection::Write(Buffer& prefix, Buffer& msg)
{
	unsigned length;
	Buffer* buffer;
	
	buffer = writer->AcquireBuffer();
	length = prefix.GetLength() + 1 + msg.GetLength();
	buffer->Writef("%u:%B:%B", length, &prefix, &msg);
	writer->Write(buffer);	
	writer->Flush();
}

void MessageConnection::WritePriority(Buffer& prefix, Buffer& msg)
{
	unsigned length;
	Buffer* buffer;
	
	buffer = writer->AcquireBuffer();
	length = prefix.GetLength() + 1 + msg.GetLength();
	buffer->Writef("%u:%B:%B", length, &prefix, &msg);
	writer->WritePriority(buffer);	
	writer->Flush();
}

void MessageConnection::OnClose()
{
	Log_Trace();
	
	if (connectTimeout.IsActive())
		return;
	
	Close();
	
	if (progress == INCOMING)
	{
		// we don't know the other side, delete conn
		transport->DeleteConnection(this);
	}
	else if (progress == READY)
	{
		// endpoint contains the other side, connect
		progress = OUTGOING;
		Connect();
	}
}

void MessageConnection::OnRead()
{
	bool			yield;
	unsigned		pos, msglength, nread, msgbegin, msgend, required;
	uint64_t		start;
	Stopwatch		sw;
	ReadBuffer		msg;

	sw.Start();
	
	Log_Trace("Read buffer: %.*s", P(tcpread.buffer));
	
	tcpread.requested = IO_READ_ANY;
	pos = 0;
	start = Now();
	yield = false;

	while(true)
	{
		msglength = BufferToUInt64(tcpread.buffer->GetBuffer() + pos,
		 tcpread.buffer->GetLength() - pos, &nread);
		
		if (msglength > tcpread.buffer->GetSize() - NumDigits(msglength) - 1)
		{
			Log_Trace();
			required = msglength + NumDigits(msglength) + 1;
			if (required > 10*MB)
			{
				Log_Trace();
				OnClose();
				return;
			}
			tcpread.buffer->Allocate(required);
			break;
		}
		
		if (nread == 0 || (unsigned) tcpread.buffer->GetLength() - pos <= nread)
			break;
			
		if (tcpread.buffer->GetCharAt(pos + nread) != ':')
		{
			Log_Trace("Message protocol error");
			OnClose();
			return;
		}
	
		msgbegin = pos + nread + 1;
		msgend = pos + nread + 1 + msglength;
		
		if ((unsigned) tcpread.buffer->GetLength() < msgend)
		{
			tcpread.requested = msgend - pos;
			break;
		}

		msg.SetBuffer(tcpread.buffer->GetBuffer() + msgbegin);
		msg.SetLength(msglength);
		OnMessage(msg);

		pos = msgend;
		
		// if the user called Close() in OnMessageRead()
		if (state != CONNECTED)
			return;
		
		if (tcpread.buffer->GetLength() == msgend)
			break;

		if (Now() - start > MESSAGING_YIELD_TIME)
		{
			// let other code run every YIELD_TIME msec
			yield = true;
			if (resumeRead.IsActive())
				ASSERT_FAIL();
			EventLoop::Add(&resumeRead);
			break;
		}
	}
	
	if (pos > 0)
	{
		memmove(tcpread.buffer->GetBuffer(), tcpread.buffer->GetBuffer() + pos,
		 tcpread.buffer->GetLength() - pos);
		tcpread.buffer->Shorten(pos);
	}
	
	if (state == CONNECTED
	 && !tcpread.active && !yield)
		IOProcessor::Add(&tcpread);

	sw.Stop();
	Log_Trace("time spent in OnRead(): %ld", sw.Elapsed());
}

void MessageConnection::OnMessage(ReadBuffer& msg)
{
	uint64_t			nodeID;
	ReadBuffer			buffer;
	MessageConnection*	dup;

	if (progress == MessageConnection::INCOMING)
	{
		msg.Readf("%U:%#R", &nodeID, &buffer);
		dup = transport->GetConnection(nodeID);
		if (dup && nodeID != this->nodeID)
		{
			if (dup->progress == READY)
			{
				Log_Trace("delete conn");
				transport->DeleteConnection(this);		// drop current node
				return;
			}
			else
			{
				Log_Trace("delete dup");
				transport->DeleteConnection(dup);		// drop dup
			}
		}
		progress = MessageConnection::READY;
		this->nodeID = nodeID;
		this->endpoint.Set(buffer);
		Log_Trace("Conn READY to node %" PRIu64 " at %s", this->nodeID, this->endpoint.ToString());
		transport->AddConnection(this);
		transport->OnIncomingConnectionReady(this->nodeID, this->endpoint);
	}
	else if (progress == MessageConnection::OUTGOING)
	{
		ASSERT_FAIL();
	}
	else
	{
		// pass msg to upper layer
		transport->OnMessage(msg);
	}
}

void MessageConnection::OnResumeRead()
{
	Log_Trace();

	OnRead();
}

void MessageConnection::Close()
{
	EventLoop::Remove(&resumeRead);
	TCPConnection::Close();	
}

void MessageConnection::Connect()
{
	Log_Trace();
	
	TCPConnection::Connect(endpoint, MESSAGING_CONNECT_TIMEOUT);
}

void MessageConnection::OnConnect()
{
	Buffer		buffer;
	ReadBuffer	rb;

	TCPConnection::OnConnect();
	
	Log_Trace("endpoint = %s", endpoint.ToString());
	
	AsyncRead();	
	
	rb = transport->GetEndpoint().ToReadBuffer();
	// send my nodeID:endpoint
	buffer.Writef("%U:%#R", transport->GetNodeID(), &rb);
	WritePriority(buffer);

	Log_Trace("Conn READY to node %" PRIu64 " at %s", nodeID, endpoint.ToString());

	progress = READY;
}

void MessageConnection::OnConnectTimeout()
{
	TCPConnection::OnConnectTimeout();
	
	Log_Trace("endpoint = %s", endpoint.ToString());
	
	Close();
	
	TCPConnection::Connect(endpoint, MESSAGING_CONNECT_TIMEOUT);
}
