#ifndef MEMORY_H
#define MEMORY_H

#include "System/Memory/nedmalloc.h"

// route C style memory management
#define malloc  nedmalloc
#define calloc  nedcalloc
#define realloc nedrealloc
#define free    nedfree

#endif
