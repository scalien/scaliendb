#include "Config.h"
#include "Common.h"
#include "Log.h"
#include <stdio.h>

Config configFile;

ConfigVar::ConfigVar(const char* name_)
{
	name.Append(name_, strlen(name_));
	numelem = 0;
	next = NULL;
}


void ConfigVar::ClearValue()
{
	numelem = 0;
	value.Clear();
}


void ConfigVar::Append(const char *value_)
{
	numelem++;
	value.Append(value_, strlen(value_) + 1);
}


bool ConfigVar::NameEquals(const char *name_)
{
	if (name.Cmp(name_))
		return true;
	return false;
}


int ConfigVar::GetIntValue(int)
{
	int			ret;
	unsigned	nread;
	char		last;
	
	ret = (int) BufferToInt64(value.GetBuffer(), value.GetLength(), &nread);

	// value is zero-terminated so we need the second char from the back
	if (nread == value.GetLength() - 2)
	{
		last = value.GetCharAt(value.GetLength() - 2);
		if (last == 'K')
			return ret * 1000;
		if (last == 'M')
			return ret * 1000 * 1000;
		if (last == 'G')
			return ret * 1000 * 1000 * 1000;
	}

	return ret;
}


const char*	ConfigVar::GetValue()
{
	return value.GetBuffer();
}


bool ConfigVar::GetBoolValue(bool defval)
{
	if (value.Cmp("yes", sizeof("yes")) ||
		value.Cmp("true", sizeof("true")) ||
		value.Cmp("on", sizeof("on")))
	{
		return true;
	}
	
	if (value.Cmp("no", sizeof("no")) ||
		value.Cmp("false", sizeof("false")) ||
		value.Cmp("off", sizeof("off")))
	{
		return false;
	}
	
	return defval;
}


int ConfigVar::GetListNum()
{
	return numelem;
}


const char* ConfigVar::GetListValue(int num, const char* defval)
{
	const char* p;
	
	if (num >= numelem)
		return defval;
	
	p = value.GetBuffer();
	for (int i = 0; i < num; i++)
	{
		p += strlen(p) + 1;
	}
	
	return p;
}


char* ParseToken(char* start, char* token, size_t)
{
	char* p;
	int len = 0;
	
	p = start;
	if (!p || !*p)
		return NULL;
	
	token[0] = '\0';
	
	// skip whitespace
skipwhite:
	while (*p <= ' ')
	{
		if (!*p || (*p == '\r' || *p == '\n'))
			return NULL;	// end of line
		p++;
	}
	
	// skip comments
	if (*p == '#')
	{
		p++;
		while (*p && (*p != '\r' || *p != '\n')) p++;
		goto skipwhite;
	}
	
	// quoted text is one token
	if (*p == '\"')
	{
		p++;
		while (true)
		{
			if (*p == '\"' || !*p)
			{
				token[len] = '\0';
				return p + 1;
			}
			token[len++] = *p++;
		}
	}
	
	if (*p == '=' || *p == ',')
	{
		token[len++] = *p;
		token[len] = '\0';
		return p + 1;
	}
		
	// regular word or number
	while (*p > ' ' && *p != ',' && *p != '#' && *p != '=')
	{
		token[len++] = *p++;
	}
	
	token[len] = '\0';
	return p;
}

bool Config::Init(const char* filename_)
{
	FILE*		fp;
	char		buffer[1024];
	char		token[1024];
	char		name[1024];
	char*		p;
	int			nline;
	ConfigVar*	var;
	
	filename = filename_;
	fp = fopen(filename, "r");
	if (!fp)
		return false;
	
	nline = 0;
	while (fgets(buffer, sizeof(buffer) - 1, fp))
	{
		buffer[sizeof(buffer) - 1] = '\0';
		nline++;
		p = ParseToken(buffer, token, sizeof(token));
		if (!p)
		{
			// empty or commented line
			continue;
		}
		
		strncpy(name, token, strlen(token));
		name[strlen(token)] = '\0';
		
		p = ParseToken(p, token, sizeof(token));
		if (!p || token[0] != '=')
		{
			// syntax error
			Log_Message("syntax error at %s, line %d", filename, nline);
			fclose(fp);
			return false;
		}
		
		var = new ConfigVar(name);
		vars.Enqueue(var);
		while (true)
		{
			p = ParseToken(p, token, sizeof(token));
			if (!p)
				break;
			if (token[0] == ',')
				continue;

			var->Append(token);
		}
	}
	
	fclose(fp);
	return true;
}

bool Config::Save()
{
	FILE*		fp;
	ConfigVar*	var;
	
	
	fp = fopen(filename, "w");
	if (!fp)
		return false;
	
	fprintf(fp, "# Automatically generated configuration file. DO NOT EDIT!\n\n");
	
	for (var = vars.First(); var != NULL; var = var->next)
	{
		fprintf(fp, "%s = \"%s", var->name.GetBuffer(), var->GetListValue(0, ""));
		for (int i = 1; i < var->numelem; i++)
			fprintf(fp, "\",\"%s", var->GetListValue(i, ""));

		fprintf(fp, "\"\n");
	}
	
	fclose(fp);
	return true;
}

void Config::Shutdown()
{
	ConfigVar*	var;

	while ((var = vars.Dequeue()) != NULL)
		delete var;
}

ConfigVar* Config::GetVar(const char* name)
{
	ConfigVar* var;
	
	var = vars.First();
	while (var)
	{
		if (var->NameEquals(name))
			return var;
		
		var = var->next;
	}
	
	return NULL;
}

int Config::GetIntValue(const char* name, int defval)
{
	ConfigVar* var;
	
	var = GetVar(name);
	if (!var)
		return defval;
	
	return var->GetIntValue(defval);
}

const char* Config::GetValue(const char* name, const char* defval)
{
	ConfigVar* var;
	
	var = GetVar(name);
	if (!var)
		return defval;
		
	return var->GetValue();
}

bool Config::GetBoolValue(const char* name, bool defval)
{
	ConfigVar* var;
	
	var = GetVar(name);
	if (!var)
		return defval;
	
	return var->GetBoolValue(defval);
}

int Config::GetListNum(const char* name)
{
	ConfigVar* var;
	
	var = GetVar(name);
	if (!var)
		return 0;

	return var->GetListNum();
}

const char* Config::GetListValue(const char* name, int num, const char* defval)
{
	ConfigVar* var;
	
	var = GetVar(name);
	if (!var)
		return defval;
	
	return var->GetListValue(num, defval);
}

void Config::SetIntValue(const char* name, int value)
{
	char		buf[CS_INT_SIZE(value)];
	
	snprintf(buf, sizeof(buf), "%d", value);
	SetValue(name, buf);
}

void Config::SetValue(const char* name, const char *value)
{
	ConfigVar*	var;

	var = GetVar(name);
	if (!var)
	{
		var = new ConfigVar(name);
		vars.Enqueue(var);
	}
	else
		var->ClearValue();

	var->Append(value);
}

void Config::SetBoolValue(const char* name, bool value)
{
	char	buf[10];
	
	snprintf(buf, sizeof(buf), "%s", value ? "true" : "false");
	SetValue(name, buf);
}

void Config::AppendListValue(const char* name, const char* value)
{
	ConfigVar*	var;

	var = GetVar(name);
	if (!var)
	{
		var = new ConfigVar(name);
		vars.Enqueue(var);
	}

	var->Append(value);
}

