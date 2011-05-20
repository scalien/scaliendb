#ifndef JSONREADER_H
#define JSONREADER_H

#include "System/Buffers/ReadBuffer.h"

/*
===============================================================================================

 JSONReader

===============================================================================================
*/

class JSONReader
{
public:
    // Low-level interface
    static bool         FindMember(ReadBuffer json, ReadBuffer name, ReadBuffer& member);
    static bool         FirstMember(ReadBuffer json, ReadBuffer& member);

    static bool         MemberValue(ReadBuffer jsonMember, ReadBuffer& value);
    static bool         MemberStringValue(ReadBuffer jsonMember, ReadBuffer& value);
    static bool         MemberInt64Value(ReadBuffer json, int64_t& value);

    static bool         GetMemberValue(ReadBuffer json, ReadBuffer name, ReadBuffer &member);
    static bool         GetObject(ReadBuffer json, ReadBuffer name, ReadBuffer& object);

    static bool         FindValue(ReadBuffer json, ReadBuffer name, ReadBuffer& value);
    static bool         GetValue(ReadBuffer json, ReadBuffer& value);
    
    static bool         GetStringValue(ReadBuffer json, ReadBuffer& value);
    static bool         GetNumberValue(ReadBuffer json, ReadBuffer& value);
    static bool         GetObjectValue(ReadBuffer json, ReadBuffer& value);
    static bool         GetArray(ReadBuffer json, ReadBuffer& value, unsigned& length);
    static bool         GetArrayElement(ReadBuffer json, unsigned index, ReadBuffer& value);
    static bool         IsTrue(ReadBuffer json);
    static bool         IsFalse(ReadBuffer json);
    static bool         IsNull(ReadBuffer json);
    static bool         GetBooleanValue(ReadBuffer json, bool& value);
    
    static bool         IsValid(ReadBuffer json);
};

/*
===============================================================================================

 JSONIterator

===============================================================================================
*/

class JSON;

class JSONIterator
{
public:
    JSONIterator();
    JSONIterator(const ReadBuffer& data, bool isPair);

    // HACK: enable FOREACH
    bool            operator!=(unsigned u);

    bool            IsPair();
    bool            GetName(ReadBuffer& name);
    bool            GetValue(JSON& value);

private:
    friend class JSON;
    
    ReadBuffer      data;
    bool            isPair;
};

/*
===============================================================================================

 JSON

===============================================================================================
*/

class JSON
{
public:
    enum Type
    {
        Undefined = 0,
        String = 1,
        Number = 2,
        Object = 3,
        Array = 4,
        True = 5,
        False = 6,
        Null = 7 
    };
        
    unsigned        GetLength();
    char*           GetBuffer();
    Type            GetType();
    void            SetType(Type type);
    
    static Type     GetJSONType(ReadBuffer json);
    static JSON     Create(ReadBuffer json);
    
    JSONIterator    First();
    JSONIterator    Next(JSONIterator it);
        
    bool            IsString();
    bool            IsNumber();
    bool            IsObject();
    bool            IsArray();
    bool            IsTrue();
    bool            IsFalse();
    bool            IsNull();
    bool            IsUndefined();
    
    bool            GetMember(ReadBuffer name, JSON& json);
    bool            GetArrayLength(unsigned& length);
    bool            GetArrayElement(unsigned index, JSON& json);
    bool            GetStringValue(ReadBuffer& value);
    bool            GetInt64Value(int64_t& value);
    
    ReadBuffer      GetReadBuffer();

private:
    ReadBuffer      data;
};

#endif
