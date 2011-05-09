#ifndef DLL_H
#define DLL_H

#ifdef _WIN32
#include <windows.h>
typedef HMODULE DLLHandle;
#else
#include <dlfcn.h>
typedef void* DLLHandle;
#endif

#ifndef DLLEXPORT
# ifdef _WIN32
#  define DLLEXPORT	__declspec(dllexport)
# else
#  define DLLEXPORT
# endif
#endif /* DLLEXPORT */

#ifndef DLLCALL
# ifdef _WIN32
#  define DLLCALL	__stdcall
# else
#  define DLLCALL
# endif
#endif /* DLLCALL */

#ifndef DLLEXPORT_BEGIN
# ifdef __cplusplus
#  define DLLEXPORT_BEGIN extern "C" {
# else
#  define DLLEXPORT_BEGIN
# endif
#endif /* DLLEXPORT_BEGIN */

#ifndef DLLEXPORT_END
# ifdef __cplusplus
#  define DLLEXPORT_END }
# else
#  define DLLEXPORT_END
# endif
#endif

/*
===============================================================================================

 DLL -- dynamic loadable libraries

===============================================================================================
*/

class DLL
{
public:
    DLL();
    ~DLL();
    
    bool            Load(const char* name, const char* ext = 0, int mode = 0);
    void            Unload();
    
    void*           GetFunction(const char* name);
    const char*     GetError();
    
private:
    DLLHandle       handle;
    char*           error;
};

#endif
