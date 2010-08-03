#ifndef DATASERVICE_H
#define DATASERVICE_H

class DataMessage; // forward

class DataService
{
public:
	virtual			~DataService() {}
	virtual void	OnComplete(DataMessage* msg, bool status) = 0;
};

#endif
