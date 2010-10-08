%module scaliendb_client
%include "stl.i"
%include "inttypes.i"
%include "stdint.i"

%{
/* Includes the header in the wrapper code */
#define SWIG_FILE_WITH_INIT
#include "../SDBPClientWrapper.h"
%}
 
/* Parse the header file to generate wrappers */
%include "../SDBPClientWrapper.h"
