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
    Deferred() {}
    Deferred(const Callable& c) : callable(c) {}
    ~Deferred() { Call(callable); }

    void        Set(const Callable& c)  { callable = c; }
    void        Unset()                 { callable.Unset(); }

private:
    Callable    callable;
};

#endif
