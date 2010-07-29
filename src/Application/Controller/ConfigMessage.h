#ifndef CONFIGMESSAGE_H
#define CONFIGMESSAGE_H

#define CONFIG_CREATE_CHUNK	'C'

class ConfigMessage
{
	char			type;

	uint64_t		chunkID;
	
	unsigned		numNodes;
	Endpoint		nodes[7];

	bool			Read(const ReadBuffer& buffer);
	bool			Write(Buffer& buffer) const;
};

#endif
