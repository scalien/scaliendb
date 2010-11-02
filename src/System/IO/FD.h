#ifndef FD_H
#define FD_H

#include "System/Platform.h"

// File descriptor abstraction

// On Windows the file handle is a pointer, on unices it is a
// growing index in an array in the process, thus it can be used
// as an array index in IOProcessor. We emulate this behavior on
// Windows.

// TODO: rename sock to handle

#ifdef _WIN32
struct FD
{
    int         index;
    intptr_t    sock;

    bool        operator==(const FD& other)
    {
        if (index == other.index && sock == other.sock)
            return true;
        return false;
    }

    bool        operator!=(const FD& other)
    {
        return !operator==(other);
    }
};

extern const FD INVALID_FD;

#else
typedef int FD;
#define INVALID_FD -1
#endif

#endif
