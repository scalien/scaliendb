/* SWIG interface file for generating wrappers */
%module scaliendb_client
%include "stl.i"
%include "inttypes.i"
%include "stdint.i"
%include "std_string.i"

#define SWIG_PYTHON_SAFE_CSTRINGS

/* Java specific byte array typemaps */
#ifdef SWIGJAVA
%include "various.i"
/* byte[] to char* definitions */
%typemap(jni) char* "jbyteArray"
%typemap(jtype) char* "byte[]"
%typemap(jstype) char* "byte[]"
%typemap(javain) char* "$javainput"
%typemap(freearg) char* ""

%typemap(in) (char*)
{
    $1 = (char*) JCALL2(GetByteArrayElements, jenv, $input, 0);
}

%typemap(argout) char*
{
    JCALL3(ReleaseByteArrayElements, jenv, $input, (jbyte*) $1, 0);
}

/* SDBP_Buffer to byte[] definitions */
%typemap(out) SDBP_Buffer
{
    $result = jenv->NewByteArray($1.len);
    jenv->SetByteArrayRegion($result, 0, $1.len, (jbyte*) $1.data);
}

%typemap(jni) SDBP_Buffer "jbyteArray"
%typemap(jtype) SDBP_Buffer "byte[]"
%typemap(jstype) SDBP_Buffer "byte[]"
%typemap(javaout) SDBP_Buffer
{
    return $jnicall;
}

#endif /* SWIGJAVA */

/* C# specific byte array typemaps */
#ifdef SWIGCSHARP
%include "typemaps.i"

%typemap(imtype) char* "IntPtr"
%typemap(cstype) char* "IntPtr"

%clear intptr_t;
%typemap(imtype) intptr_t "IntPtr"
%typemap(cstype) intptr_t "IntPtr"
%typemap(csout) intptr_t "IntPtr"
%typemap(directorout) intptr_t "IntPtr"

%typemap(csvarout, excode=SWIGEXCODE2) intptr_t %{
    /* HACKED SWIG csvarout typemap in scaliendb_client.i  */
    get {
		return $imcall;
    } %}


%csmethodmodifiers "public unsafe";


#endif /* SWIGCSHARP */


%{
/* Includes the header in the wrapper code */
#define SWIG_FILE_WITH_INIT
#include "../SDBPClientWrapper.h"
%}
 
/* Parse the header file to generate wrappers */
%include "../SDBPClientWrapper.h"
