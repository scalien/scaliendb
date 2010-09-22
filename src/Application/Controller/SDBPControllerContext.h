#ifndef SDBPCONTROLLERCONTEXT_H
#define SDBPCONTROLLERCONTEXT_H

#include "Application/SDBP/SDBPContext.h"
#include "State/ConfigState.h"
#include "ConfigMessage.h"

class Controller;

/*
===============================================================================================

 SDBPControllerContext

===============================================================================================
*/

class SDBPControllerContext : public SDBPContext
{
public:
	void					SetController(Controller* controller);
	
		
private:
	SDBPConnection*			connection;
	Controller*				controller;
	uint64_t				getConfigStateCommandID;
};


#endif
