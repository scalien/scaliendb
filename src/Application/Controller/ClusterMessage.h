#ifndef CLUSTERMESSAGE_H
#define CLUSTERMESSAGE_H

#define CLUSTER_PROTOCOL_ID		'C'

#define CLUSTER_HELLO			'H'
#define CLUSTER_INFO			'I'

class ClusterMessage
{
public:
	char		type;

	bool		Read(const ReadBuffer& buffer);
	bool		Write(Buffer& buffer) const;
};

#endif
