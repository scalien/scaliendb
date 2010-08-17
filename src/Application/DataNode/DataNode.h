#ifndef DATANODE_H
#define DATANODE_H

#include "DataChunkContext.h"
#include "DataHTTPHandler.h"
#include "DataService.h"

class DataNode
{
public:
	void				Init(Table* table);

	DataService*		GetService();
	HTTPHandler*		GetHandler();
	
private:
	Table*				table;
	uint64_t			nodeID;
	DataHTTPHandler		httpHandler;
};

#endif
