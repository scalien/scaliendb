#ifndef CONFIG_H
#define CONFIG_H

#include "Buffers/Buffer.h"
#include "Containers/InQueue.h"
#include "Platform.h"

/*
===============================================================================================

 ConfigVar

===============================================================================================
*/

class ConfigVar
{
public:
    ConfigVar(const char* name_);
    
    void                ClearValue();
    void                Append(const char *value_);
    bool                NameEquals(const char *name_);
    int                 GetIntValue(int defval);
    int64_t             GetInt64Value(int64_t defval);
    const char*         GetValue(); 
    bool                GetBoolValue(bool defval);
    int                 GetListNum();
    const char*         GetListValue(int num, const char* defval);

    ConfigVar*          next;
    Buffer              name;
    Buffer              value;
    int                 numelem;
};

/*
===============================================================================================

 Config

===============================================================================================
*/

class Config
{
public:
    bool                Init(const char* filename);
    bool                Save();
    void                Shutdown();
    
    int                 GetIntValue(const char *name, int defval);
    int64_t             GetInt64Value(const char *name, int64_t defval);
    const char*         GetValue(const char* name, const char *defval);
    bool                GetBoolValue(const char* name, bool defval);
    
    int                 GetListNum(const char* name);
    const char*         GetListValue(const char* name, int num, const char* defval);
    
    void                SetIntValue(const char* name, int value);
    void                SetInt64Value(const char* name, int64_t value);
    void                SetValue(const char* name, const char *value);
    void                SetBoolValue(const char* name, bool value);
    void                AppendListValue(const char* name, const char* value);

    ConfigVar*          First();
    ConfigVar*          Next(ConfigVar* var);

private:
    ConfigVar*          GetVar(const char* name);

    const char*         filename;
    InQueue<ConfigVar>  vars;
};

// global config file
extern Config configFile;

#endif
