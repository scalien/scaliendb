#ifndef COUNTDOWN_H
#define COUNTDOWN_H

#include "Timer.h"

/*
===============================================================================================

 Countdown

===============================================================================================
*/

class Countdown : public Timer
{
public:
    Countdown();    
        
    void            SetDelay(uint64_t delay);
    uint64_t        GetDelay() const;

    virtual void    OnAdd();
    
private:
    uint64_t        delay;
};

#endif
