#ifndef STATEPROCESSOR_H
#define STATEPROCESSOR_H

#include	"State.h"

/*
===============================================================================

 StateProcessor

===============================================================================
*/

class StateProcessor
{
public:
	void	UpdateConfig(ConfigCommand& command);
	void	UpdateCluster(ClusterMessage& msg);

	bool	GetConfigMessage(ConfigCommand& msg);
	bool	GetClusterMessage(ClusterMessage& msg);

private:
	State		state;
};

#endif
