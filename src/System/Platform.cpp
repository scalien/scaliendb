#include "Platform.h"

#ifdef _WIN32
#include <time.h>
#include <windows.h>
#else
#include <sys/time.h>
#include <pwd.h>
#include <unistd.h>
#endif

bool ChangeUser(const char *user)
{
#ifdef _WIN32
	// cannot change user on Windows
	return true;
#else
	if (user != NULL && *user && (getuid() == 0 || geteuid() == 0)) 
	{
		struct passwd *pw = getpwnam(user);
		if (!pw)
			return false;
		
		setuid(pw->pw_uid);
	}

	return true;
#endif
}
