#ifndef JOB_H
#define JOB_H

#include "System/Events/Callable.h"
#include "System/Containers/List.h"

/*
===============================================================================================

 Job

===============================================================================================
*/

class Job
{
public:
    
    Job()               { next = this; }
    virtual ~Job()      {}


    virtual void        Execute()      = 0;
    virtual void        OnComplete()   = 0;

    Job*                next;
};

#endif
