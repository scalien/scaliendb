#ifndef CONFIG_H
#define CONFIG_H

class Config
{
public:
	static bool			Init(const char* filename);
	static void			Shutdown();
	
	static int			GetIntValue(const char *name, int defval);
	static const char*	GetValue(const char* name, const char *defval);
	static bool			GetBoolValue(const char* name, bool defval);
	
	static int			GetListNum(const char* name);
	static const char*	GetListValue(const char* name, int num,
						 const char* defval);	
};

#endif
