
#define CLUSTER_PROTOCOL_ID		'C'

#define CLUSTER_HELLO			'H'
#define CLUSTER_INFO			'I'

class ClusterMessage
{
public:
	char		type;
	
	uint64_t	nodeID;
	// TODO: stuff for CLUSTER_INFO

	bool		Read(const ReadBuffer& buffer);
	bool		Write(Buffer& buffer) const;
};