#ifndef APPLICATION_H
#define APPLICATION_H

/*
===============================================================================================

 Application interface

===============================================================================================
*/

class Application
{
public:
    virtual ~Application() {}
    
    virtual void    Init() = 0;
    virtual void    Shutdown() = 0;
};

#endif
