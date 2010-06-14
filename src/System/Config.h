#ifndef CONFIG_H
#define CONFIG_H

#include "Buffers/DynArray.h"
#include "Containers/Queue.h"


class ConfigVar
{
public:
	ConfigVar(const char* name_);
	
	void			ClearValue();
	void			Append(const char *value_);
	bool			NameEquals(const char *name_);
	int				GetIntValue(int);
	const char*		GetValue();	
	bool			GetBoolValue(bool defval);
	int				GetListNum();
	const char*		GetListValue(int num, const char* defval);

	ConfigVar*		next;
	DynArray<256>	name;
	DynArray<256>	value;
	int				numelem;
};


class ConfigFile
{
public:
	bool			Init(const char* filename);
	bool			Save();
	void			Shutdown();
	
	int				GetIntValue(const char *name, int defval);
	const char*		GetValue(const char* name, const char *defval);
	bool			GetBoolValue(const char* name, bool defval);
	
	int				GetListNum(const char* name);
	const char*		GetListValue(const char* name, int num, const char* defval);
	
	void			SetIntValue(const char* name, int value);
	void			SetValue(const char* name, const char *value);
	void			SetBoolValue(const char* name, bool value);
	void			AppendListValue(const char* name, const char* value);

	const char*		filename;
private:
	typedef Queue<ConfigVar, &ConfigVar::next> VarQueue;

	VarQueue		vars;

	ConfigVar*		GetVar(const char* name);
};


#endif
