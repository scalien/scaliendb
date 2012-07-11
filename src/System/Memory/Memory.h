#ifndef MEMORY_H
#define MEMORY_H

/*

  nedmalloc is a VERY fast, VERY scalable, multithreaded
  memory allocator with little memory fragmentation.

  See http://www.nedprod.com/programs/portable/nedmalloc/

*/

#include "System/Memory/nedmalloc.h"

// route C style memory management
#define malloc  nedmalloc
#define calloc  nedcalloc
#define realloc nedrealloc
#define free    nedfree

#endif
