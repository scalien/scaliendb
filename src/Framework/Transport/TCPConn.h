#ifndef TCPCONN_H
#define TCPCONN_H

#include "System/Events/Callable.h"
#include "System/Events/CdownTimer.h"
#include "System/Buffers/DynArray.h"
#include "System/Containers/Queue.h"
#include "System/IO/Socket.h"
#include "System/IO/IOOperation.h"

#define TCP_CONNECT_TIMEOUT 3000
#define TCP_BUFFER_SIZE		8196

class TCPConn
{
public:
	enum State		{ DISCONNECTED, CONNECTED, CONNECTING };

	TCPConn();
	virtual ~TCPConn();
	
	void			Init(bool startRead = true);
	void			Connect(Endpoint &endpoint_, unsigned timeout);
	void			Close();

	Socket&			GetSocket() { return socket; }
	const State		GetState() { return state; }
	
	unsigned		BytesQueued();

	void			AsyncRead(bool start = true);
	void			Write(const char* data, int count, bool flush = true);
	
	TCPConn			*next;

protected:
	typedef DynArray<TCP_BUFFER_SIZE> Buffer;
	typedef Queue<Buffer, &Buffer::next> BufferQueue;

	
	State			state;
	Socket			socket;
	TCPRead			tcpread;
	TCPWrite		tcpwrite;
	Buffer			readBuffer;
	BufferQueue		writeQueue;
	CdownTimer		connectTimeout;
	
	MFunc<TCPConn>	onRead;
	MFunc<TCPConn>	onWrite;
	MFunc<TCPConn>	onClose;
	MFunc<TCPConn>	onConnect;
	MFunc<TCPConn>	onConnectTimeout;
	
	virtual void	OnRead() = 0;
	virtual void	OnWrite();
	virtual void	OnClose() = 0;
	virtual void	OnConnect();
	virtual void	OnConnectTimeout();
	
	void			Append(const char* buffer, unsigned length);
	void			WritePending();
};

#endif
