#include "DLL.h"
#include "System/Buffers/Buffer.h"

#ifdef PLATFORM_WINDOWS
	const char *DLLExtension = ".dll";
#else
    #ifdef PLATFORM_DARWIN
        const char *DLLExtension = ".dylib";
    #else
        const char *DLLExtension = ".so";
    #endif
#endif

DLL::DLL()
{
    handle = NULL;
    error = NULL;
}

DLL::~DLL()
{
    Unload();
}

// TODO: modes
bool DLL::Load(const char *name, const char* ext, int mode)
{
    Buffer  fullname;

	error = NULL;

    if (ext == NULL)
        ext = DLLExtension;

    fullname.Writef("%s%s", name, ext);
    fullname.NullTerminate();

#ifdef _WIN32
	handle = LoadLibrary(fullname.GetBuffer());
#else
	if (mode == 0) mode = RTLD_NOW;
	handle = dlopen(fullname.GetBuffer(), mode);
#endif
    if (handle == NULL)
        return false;
    return true;
}

void DLL::Unload()
{
	if (handle)
    {
#ifdef _WIN32
		FreeLibrary(handle);
#else
		dlclose(handle);
#endif
        handle = NULL;
	}

#ifdef _WIN32
	if (error) 
    {
        LocalFree(error);
        error = NULL;
    }
#endif
}

void* DLL::GetFunction(const char* name)
{
    void*   ret;
    
#ifdef _WIN32
	ret = GetProcAddress(handle, name);
	if (ret == 0)
    {
		FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER |
				FORMAT_MESSAGE_FROM_SYSTEM | 
				FORMAT_MESSAGE_IGNORE_INSERTS,
				NULL,
				GetLastError(),
				MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
				(LPTSTR)&error,
				0,
				NULL);
	}
	return ret;
#else
	dlerror(); // clear error code
	ret = dlsym(handle, name);
	if ((error = dlerror()) != NULL)
		return NULL;
	else
        return ret;
#endif
}

const char* DLL::GetError()
{
    return error;
}
