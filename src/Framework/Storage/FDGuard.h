#ifndef FDGUARD_H
#define FDGUARD_H

#include "System/IO/FD.h"

/*
===============================================================================================

 FDGuard

===============================================================================================
*/

class FDGuard
{
public:
    FDGuard();    
    ~FDGuard();

    FD Open(const char* filename, int mode);
    void Close();

    FD GetFD();

private:
    FD fd;
};

#endif
