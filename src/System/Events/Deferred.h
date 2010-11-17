#ifndef DEFERRED_H
#define DEFERRED_H

#include "Callable.h"

/*
===============================================================================================

 Deferred

===============================================================================================
*/

class Deferred
{
public:
    ~Deferred() { Call(callable); }

    void        Set(Callable c) { callable = c; }

private:
    Callable    callable;
};

#endif
