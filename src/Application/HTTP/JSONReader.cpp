#include "JSONReader.h"

#include <ctype.h>
#include <stdlib.h>

#define JSON_NULL       "null"
#define JSON_TRUE       "true"
#define JSON_FALSE      "false"

const char* JSON_ERROR = "";


#define JSON_IS_FIRST_CHAR_RETURN(json, chr, ret)   \
{                                                   \
    if ((json).GetLength() == 0)                    \
        return ret;                                 \
    if ((json).GetCharAt(0) != chr)                 \
        return ret;                                 \
}

/*
===============================================================================================

 JSONReader

===============================================================================================
*/

ReadBuffer JSON_Skip(ReadBuffer json, unsigned count);
ReadBuffer JSON_SkipValue(ReadBuffer json, unsigned count);

ReadBuffer JSON_SkipWhitespace(ReadBuffer json)
{
    char*   p;
    
    if (json.GetLength() == 0)
        return json;
    
	p = json.GetBuffer();
	while (*p == 0x20 || *p == 0x09 || *p == 0x0A || *p == 0x0D)
    {
        json.Advance(1);
        if (json.GetLength() == 0)
            return json;
        p = json.GetBuffer();
    }
	
	return json;
}

ReadBuffer JSON_SkipChar(ReadBuffer json, char c)
{
    if (json.GetLength() == 0 || json.GetCharAt(0) != c)
        return JSON_ERROR;

    json.Advance(1);
    json = JSON_SkipWhitespace(json);

    return json;
}

ReadBuffer JSON_SkipNumber(ReadBuffer json)
{
    if (json.GetLength() == 0)
        return json;
    
    if (json.GetCharAt(0) == '-' || isdigit(json.GetCharAt(0)))
        json.Advance(1);
    else
        return JSON_ERROR;
    
    while (json.GetLength() > 0 && isdigit(json.GetCharAt(0)))
        json.Advance(1);
    
    if (json.GetLength() > 0 && json.GetCharAt(0) == '.')
    {
        json.Advance(1);
        while (json.GetLength() > 0 && isdigit(json.GetCharAt(0)))
            json.Advance(1);        
    }
    
    if (json.GetLength() > 0 && (json.GetCharAt(0) == 'e' || json.GetCharAt(0) == 'E'))
    {
        json.Advance(1);
        if (json.GetLength() > 0 && (json.GetCharAt(0) == '+' || json.GetCharAt(0) == '-'))
            json.Advance(1);
        while (json.GetLength() > 0 && isdigit(json.GetCharAt(0)))
            json.Advance(1);        
    }
    
    return json;
}

ReadBuffer JSON_SkipNull(ReadBuffer json)
{
    if (json.GetLength() < CSLLEN(JSON_NULL))
        return JSON_ERROR;
    
    if (strncmp(json.GetBuffer(), JSON_NULL, CSLLEN(JSON_NULL)) == 0)
    {
        json.Advance(CSLLEN(JSON_NULL));
        return json;
    }
    
    return JSON_ERROR;
}

ReadBuffer JSON_SkipTrue(ReadBuffer json)
{
    if (json.GetLength() < CSLLEN(JSON_TRUE))
        return JSON_ERROR;
    
    if (strncmp(json.GetBuffer(), JSON_TRUE, CSLLEN(JSON_TRUE)) == 0)
    {
        json.Advance(CSLLEN(JSON_TRUE));
        return json;
    }
    
    return JSON_ERROR;
}

ReadBuffer JSON_SkipFalse(ReadBuffer json)
{
    if (json.GetLength() < CSLLEN(JSON_FALSE))
        return JSON_ERROR;
    
    if (strncmp(json.GetBuffer(), JSON_FALSE, CSLLEN(JSON_FALSE)) == 0)
    {
        json.Advance(CSLLEN(JSON_FALSE));
        return json;
    }
    
    return JSON_ERROR;
}

ReadBuffer JSON_SkipString(ReadBuffer json)
{
    char    prev;
    
    if (json.GetLength() == 0)
        return JSON_ERROR;
    
    if (json.GetCharAt(0) != '\"')
        return JSON_ERROR;
    
    prev = json.GetCharAt(0);
    json.Advance(1);
    while (json.GetLength() > 0)
    {
        if (json.GetCharAt(0) == '\"' && prev != '\\')
            break;

        prev = json.GetCharAt(0);
        json.Advance(1);
    }
    
    if (json.GetLength() == 0)
        return JSON_ERROR;
    
    json.Advance(1);
    
    return json;
}

ReadBuffer JSON_SkipArray(ReadBuffer json)
{
    json = JSON_SkipChar(json, '[');

    while (json.GetLength() > 0 && json.GetCharAt(0) != ']')
    {
        json = JSON_SkipValue(json, 1);
        if (json.GetLength() > 0 && json.GetCharAt(0) != ']')
            json = JSON_SkipChar(json, ',');
    }
    
    json = JSON_SkipChar(json, ']');
    
    return json;
}

ReadBuffer JSON_SkipMember(ReadBuffer json)
{
    // skip member name
    json = JSON_SkipString(json);

    // skip colon
    json = JSON_SkipChar(json, ':');
    
    // skip member value
    json = JSON_SkipValue(json, 1);
    
    return json;
}

ReadBuffer JSON_SkipObject(ReadBuffer json)
{
    json = JSON_SkipChar(json, '{');
    
    while (json.GetLength() > 0 && json.GetCharAt(0) != '}')
    {
        json = JSON_SkipMember(json);
        // skip optional comma
        if (json.GetLength() > 0 && json.GetCharAt(0) != '}')
            json = JSON_SkipChar(json, ',');
    }
        
    json = JSON_SkipChar(json, '}');
        
    return json;
}

ReadBuffer JSON_SkipValue(ReadBuffer json, unsigned count)
{
    while (count > 0)
    {
        if (json.GetLength() == 0)
            return JSON_ERROR;

        switch (json.GetCharAt(0))
        {
            case '\"':
                json = JSON_SkipString(json);
                break;
            case '{':
                json = JSON_SkipObject(json);
                break;
            case '[':
                json = JSON_SkipArray(json);
                break;
            case 't':
                json = JSON_SkipTrue(json);
                break;
            case 'f':
                json = JSON_SkipFalse(json);
                break;
            case 'n':
                json = JSON_SkipNull(json);
                break;
            case '0':
            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
            case '8':
            case '9':
            case '-':
                json = JSON_SkipNumber(json);
                break;
            case ' ':
            case '\n':
            case '\r':
            case '\t':
                json = JSON_SkipWhitespace(json);
                // HACK for skipping whitespace
                count++;
                break;
            default:
                return JSON_ERROR;
        }        
        count--;
    }

    return json;
}

ReadBuffer JSON_Skip(ReadBuffer json, unsigned count)
{
    while (count > 0)
    {
        if (json.GetLength() == 0)
            return JSON_ERROR;
    
        switch (json.GetCharAt(0))
        {
            case '\"':
                json = JSON_SkipString(json);
                break;
            case '{':
                json = JSON_SkipObject(json);
                break;
            case '}':
                json.Advance(1);
                break;
            case '[':
                json = JSON_SkipArray(json);
                break;
            case ']':
                json.Advance(1);
                break;
            case ',':
                json.Advance(1);
                break;
            case ':':
                json.Advance(1);
                break;
            case 't':
                json = JSON_SkipTrue(json);
                break;
            case 'f':
                json = JSON_SkipFalse(json);
                break;
            case 'n':
                json = JSON_SkipNull(json);
                break;
            case '0':
            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
            case '8':
            case '9':
            case '-':
                json = JSON_SkipNumber(json);
                break;
            case ' ':
            case '\n':
            case '\r':
            case '\t':
                json = JSON_SkipWhitespace(json);
                // HACK for skipping whitespace
                count++;
                break;
            default:
                return JSON_ERROR;
        }        
        count--;
    }
    
    return json;
}

static bool JSON_MemberIsObject(ReadBuffer json)
{
    json = JSON_SkipString(json);
    if (json.GetBuffer() == JSON_ERROR)
        return false;

    JSON_IS_FIRST_CHAR_RETURN(json, ':', false);
    json = JSON_Skip(json, 1);

    json = JSON_Skip(json, 1);
    if (json.GetBuffer() == JSON_ERROR)
        return false;
    
    return true;
}

static bool JSON_IsIdentifierStart(char c)
{
    if (isalpha(c) || c == '_' || c == '$')
        return true;
    return false;
}

static bool JSON_IsIdentifierChar(char c)
{
    if (isalnum(c) || c == '_' || c == '$')
        return true;
    return false;
}

static ReadBuffer JSON_SkipIdentifier(ReadBuffer name)
{
    if (name.GetLength() == 0)
        return JSON_ERROR;
    
    if (!JSON_IsIdentifierStart(name.GetCharAt(0)))
        return JSON_ERROR;
    
    name.Advance(1);
    while (name.GetLength() > 0 && JSON_IsIdentifierChar(name.GetCharAt(0)))
        name.Advance(1);
    
    return name;
}

static bool JSON_GetStringValue(ReadBuffer json, ReadBuffer& value)
{
    ReadBuffer  end;
    
    if (json.GetLength() == 0 || json.GetCharAt(0) != '\"')
        return false;
    
    end = JSON_SkipString(json);
    if (end.GetBuffer() == JSON_ERROR)
        return false;

    json.Advance(1);
    value.SetBuffer(json.GetBuffer());
    value.SetLength(end.GetBuffer() - json.GetBuffer() - 1);
    return true;
}

static bool JSON_GetInt64(ReadBuffer json, ReadBuffer& value, int64_t& number)
{
    int64_t     ret;
    ReadBuffer  tmp;
    
    if (json.GetLength() == 0 || !isdigit(json.GetCharAt(0)))
        return false;
    
    tmp = json;
    ret = 0;
    while (tmp.GetLength() > 0 && isdigit(tmp.GetCharAt(0)))
    {
        ret = ret * 10 + tmp.GetCharAt(0) - '0';
        tmp.Advance(1);
    }
    
    value.SetBuffer(json.GetBuffer());
    value.SetLength(tmp.GetBuffer() - json.GetBuffer());
    number = ret;
    
    return true;
}

static bool JSON_GetNamePart(ReadBuffer& name, ReadBuffer& part)
{
    ReadBuffer  tmp;
    
    if (name.GetLength() == 0)
        return false;
    
    if (name.GetCharAt(0) == '.')
    {
        name.Advance(1);
        tmp = JSON_SkipIdentifier(name);
        if (tmp.GetBuffer() == JSON_ERROR)
            return false;
        part.SetBuffer(name.GetBuffer());
        part.SetLength(tmp.GetBuffer() - name.GetBuffer());
        name.Advance(part.GetLength());
        return true;
    }
    else if (name.GetCharAt(0) == '[')
    {
        name.Advance(1);
        name = JSON_SkipWhitespace(name);
        if (name.GetLength() == 0)
            return false;

        if (name.GetCharAt(0) == '\"')
        {
            // string index
            tmp = JSON_SkipString(name);
            if (tmp.GetLength() == 0 || tmp.GetCharAt(0) != ']')
                return false;
            name.Advance(1);
            part.SetBuffer(name.GetBuffer());
            part.SetLength(tmp.GetBuffer() - name.GetBuffer() - 1);
            name.Advance(part.GetLength() + 2);
            return true;
        }
        else if (isdigit(name.GetCharAt(0)))
        {
            // numeric index
            tmp = name;
            while (tmp.GetLength() > 0 && isdigit(tmp.GetCharAt(0)))
                tmp.Advance(1);
            if (tmp.GetLength() == 0 || tmp.GetCharAt(0) != ']')
                return false;
            part.SetBuffer(name.GetBuffer());
            part.SetLength(tmp.GetBuffer() - name.GetBuffer());
            name.Advance(part.GetLength() + 1);
            return true;
        }
        else
            return false;

    }
    else if (JSON_IsIdentifierStart(name.GetCharAt(0)))
    {
        tmp = JSON_SkipIdentifier(name);
        if (tmp.GetBuffer() == JSON_ERROR)
            return false;
        
        part.SetBuffer(name.GetBuffer());
        part.SetLength(tmp.GetBuffer() - name.GetBuffer());
        name.Advance(part.GetLength());
        return true;
    }

    return false;
}

bool JSONReader::FindValue(ReadBuffer json, ReadBuffer name, ReadBuffer &value)
{
    ReadBuffer  namepart;
    ReadBuffer  tmp;
    ReadBuffer  end;
    int64_t     index;
    
    if (json.GetLength() == 0)
    {
        value = JSON_ERROR;
        return false;
    }
    
    while (name.GetLength() > 0)
    {
        if (!JSON_GetNamePart(name, namepart))
            return false;

        if (JSON_GetInt64(namepart, tmp, index))
        {
            // numeric index
            if (json.GetLength() == 0 || json.GetCharAt(0) != '[')
                return false;
            json.Advance(1);
            json = JSON_SkipValue(json, (unsigned) index);
            if (json.GetLength() > 0 && json.GetCharAt(0) == ',')
                json.Advance(1);
        }
        else
        {
            if (!FirstMember(json, json))
                return false;

            // string index
            while (json.GetLength() > 0)
            {
                if (!JSON_GetStringValue(json, tmp))
                    return false;

                if (ReadBuffer::Cmp(namepart, tmp) == 0)
                {
                    if (!MemberValue(json, json))
                        return false;
                    break;
                }

                json = JSON_SkipMember(json);
                if (json.GetBuffer() == JSON_ERROR)
                    return false;
            }
        }
    }
    
    if (json.GetLength() == 0)
        return false;
    
    tmp = JSON_SkipValue(json, 1);
    if (tmp.GetBuffer() == JSON_ERROR)
        return false;
    
    value.SetBuffer(json.GetBuffer());
    value.SetLength(tmp.GetBuffer() - json.GetBuffer());

    return true;
}

bool JSONReader::GetValue(ReadBuffer json, ReadBuffer& value)
{
    ReadBuffer  tmp;

    tmp = JSON_SkipValue(json, 1);
    if (tmp.GetBuffer() == JSON_ERROR)
        return false;
    
    value.SetBuffer(json.GetBuffer());
    value.SetLength(tmp.GetBuffer() - json.GetBuffer());
    
    return true;
}

bool JSONReader::FindMember(ReadBuffer json, ReadBuffer name, ReadBuffer &member)
{
    ReadBuffer  namepart;
    ReadBuffer  tmp;
    ReadBuffer  end;
    int64_t     index;
    
    if (json.GetLength() == 0)
    {
        member = "";
        return false;
    }
    
    while (name.GetLength() > 0)
    {
        if (!JSON_GetNamePart(name, namepart))
            return false;

        if (JSON_GetInt64(namepart, tmp, index))
        {
            // numeric index
            if (json.GetLength() == 0 || json.GetCharAt(0) != '[')
                return false;
            json.Advance(1);
            json = JSON_SkipValue(json, (unsigned) index);
            if (json.GetLength() > 0 && json.GetCharAt(0) == ',')
                json.Advance(1);
        }
        else
        {
            if (!FirstMember(json, json))
                return false;

            // string index
            while (json.GetLength() > 0)
            {
                if (!JSON_GetStringValue(json, tmp))
                    return false;

                if (ReadBuffer::Cmp(namepart, tmp) == 0)
                {
                    if (name.GetLength() > 0)
                    {
                        if (!MemberValue(json, json))
                            return false;
                    }

                    break;
                }

                json = JSON_SkipMember(json);
                if (json.GetBuffer() == JSON_ERROR)
                    return false;
            }
        }
    }
    
    member = json;
    return true;
}

bool JSONReader::FirstMember(ReadBuffer json, ReadBuffer& member)
{
    if (json.GetLength() == 0)
        return false;
    
    if (json.GetCharAt(0) != '{')
        return false;
    
    json.Advance(1);
    json = JSON_SkipWhitespace(json);
    if (json.GetLength() == 0 || json.GetCharAt(0) != '\"')
        return false;
    
    member = json;
    return true;
}

bool JSONReader::MemberValue(ReadBuffer json, ReadBuffer& value)
{
    ReadBuffer  tmp;

    if (json.GetLength() == 0 || json.GetCharAt(0) != '\"')
        return false;

    json = JSON_SkipString(json);
    json = JSON_SkipChar(json, ':');
    if (json.GetBuffer() == JSON_ERROR)
        return false;
    
    value = json;
    return true;
}

bool JSONReader::MemberStringValue(ReadBuffer json, ReadBuffer& value)
{
    ReadBuffer  member;
    
    if (!MemberValue(json, member))
        return false;

    if (!JSON_GetStringValue(member, value))
        return false;
        
    return true;
}

bool JSONReader::MemberInt64Value(ReadBuffer json, int64_t& value)
{
    ReadBuffer  member;
    ReadBuffer  tmp;
    int64_t     ret;
    
    if (!MemberValue(json, member))
        return false;

    tmp = JSON_Skip(member, 1);
    if (tmp.GetBuffer() == JSON_ERROR)
        return false;
    
    ret = 0;
    while (member.GetLength() > 0 && isdigit(member.GetCharAt(0)))
    {
        ret = ret * 10 + member.GetCharAt(0) - '0';
        member.Advance(1);
    }
    
    member = JSON_SkipWhitespace(member);
    if (member.GetBuffer() != tmp.GetBuffer())
        return false;

    value = ret;
    return true;
}

bool JSONReader::GetMemberValue(ReadBuffer json, ReadBuffer name, ReadBuffer& value)
{
    ReadBuffer  member;
    
    if (!FindMember(json, name, member))
        return false;
    
    return MemberValue(member, value);
}

bool JSONReader::GetObject(ReadBuffer json, ReadBuffer name, ReadBuffer& object)
{
    if (json.GetLength() == 0)
        return false;
    
    json = JSON_SkipWhitespace(json);
    if (json.GetLength() == 0)
        return false;
    
    if (json.GetCharAt(0) != '{')
        return false;
    
    json = JSON_Skip(json, 1);
    if (!FindMember(json, name, json))
        return false;
    
    if (!JSON_MemberIsObject(json))
        return false;

    // skip name and colon
    json = JSON_SkipString(json);
    json = JSON_SkipWhitespace(json);
    if (json.GetLength() == 0 || json.GetCharAt(0) != ':')
        return false;
    json = JSON_Skip(json, 1);
    
    object = json;
    return true;
}

bool JSONReader::GetStringValue(ReadBuffer json, ReadBuffer& value)
{
    return JSON_GetStringValue(json, value);
}

bool JSONReader::GetArray(ReadBuffer json, ReadBuffer& value, unsigned& length)
{
    ReadBuffer  tmp;
    unsigned    count;
    
    if (json.GetLength() == 0 || json.GetCharAt(0) != '[')
        return false;

    tmp = json;
    tmp.Advance(1);

    count = 0;
    while (tmp.GetLength() > 0 && tmp.GetCharAt(0) != ']')
    {
        tmp = JSON_SkipValue(tmp, 1);
        count++;
        if (tmp.GetLength() > 0 && tmp.GetCharAt(0) == ',')
        {
            tmp.Advance(1);
            tmp = JSON_SkipValue(tmp, 1);
            count++;
        }
    }
    
    if (tmp.GetBuffer() == JSON_ERROR)
        return false;
    
    tmp.Advance(1);
    value.SetBuffer(json.GetBuffer());
    value.SetLength(tmp.GetBuffer() - json.GetBuffer());
    length = count;
    
    return true;
}

bool JSONReader::GetArrayElement(ReadBuffer json, unsigned index, ReadBuffer& value)
{
    ReadBuffer  tmp;
    
    json = JSON_SkipChar(json, '[');
    json = JSON_SkipValue(json, index);
    if (json.GetLength() > 0 && json.GetCharAt(0) == ',')
        json = JSON_SkipChar(json, ',');
    
    tmp = JSON_SkipValue(json, 1);
    if (tmp.GetBuffer() == JSON_ERROR)
        return false;

    value.SetBuffer(json.GetBuffer());
    value.SetLength(tmp.GetBuffer() - json.GetBuffer());

    return true;
}

bool JSONReader::IsValid(ReadBuffer json)
{
    if (json.GetLength() == 0)
        return false;
    
    if (json.GetCharAt(0) == '[')
    {
        json = JSON_SkipArray(json);
        if (json.GetBuffer() != JSON_ERROR)
            return true;
        return false;
    }
    
    if (json.GetCharAt(0) == '{')
    {
        json = JSON_SkipObject(json);
        if (json.GetBuffer() != JSON_ERROR)
            return true;
        return false;
    }
    
    return false;
}

/*
===============================================================================================

 JSONIterator

===============================================================================================
*/

JSONIterator::JSONIterator()
{
    isPair = false;
}

JSONIterator::JSONIterator(const ReadBuffer& data_, bool isPair_)
{
    data = data_;
    isPair = isPair_;
}

bool JSONIterator::operator!=(unsigned u)
{
    ASSERT(u == 0);
    if (data.GetLength() == 0)
        return false;
    return true;
}

bool JSONIterator::IsPair()
{
    return isPair;
}

bool JSONIterator::GetName(ReadBuffer& name)
{
    if (!isPair)
        return false;
    
    return JSONReader::GetStringValue(data, name);
}

bool JSONIterator::GetValue(JSON& json)
{
    ReadBuffer  tmp;
    
    if (!isPair)
    {
        if (!JSONReader::GetValue(data, tmp))
            return false;
        
        json = JSON::Create(tmp);
        return true;
    }

    if (!JSONReader::MemberValue(data, tmp))
        return false;
    
    json = JSON::Create(tmp);
    return true;
}

/*
===============================================================================================

 JSON

===============================================================================================
*/

unsigned JSON::GetLength()
{
    return data.GetLength() & 0x0FFFFFFFU;
}

char* JSON::GetBuffer()
{
    return data.GetBuffer();
}

JSON::Type JSON::GetType()
{
    return (JSON::Type) ((data.GetLength() & 0xF0000000U) >> 28);
}

void JSON::SetType(JSON::Type type)
{
    data.SetLength(GetLength() | ((type & 0x0F) << 28));
}

JSON::Type JSON::GetJSONType(ReadBuffer json)
{
    json = JSON_SkipWhitespace(json);
    
    if (json.GetLength() == 0)
        return JSON::Undefined;

    switch (json.GetCharAt(0))
    {
        case '\"':
            return JSON::String;
        case '{':
            return JSON::Object;
        case '[':
            return JSON::Array;
        case 't':
            return JSON::True;
        case 'f':
            return JSON::False;
        case 'n':
            return JSON::Null;
        case '0':
        case '1':
        case '2':
        case '3':
        case '4':
        case '5':
        case '6':
        case '7':
        case '8':
        case '9':
        case '-':
            return JSON::Number;
        default:
            return JSON::Undefined;
    }
}

JSON JSON::Create(ReadBuffer json)
{
    JSON    ret;
    
    ret.data = json;
    ret.SetType(GetJSONType(json));
    
    return ret;
}

JSONIterator JSON::First()
{
    JSONIterator    end;
    ReadBuffer      tmp;
    ReadBuffer      endBracket;
    ReadBuffer      value;

    if (IsArray())
    {
        tmp = JSON_SkipChar(GetReadBuffer(), '[');
        if (tmp.GetBuffer() == JSON_ERROR)
            return end;
        // TODO: error handling
        endBracket = JSON_SkipChar(tmp, ']');
        if (endBracket.GetBuffer() != JSON_ERROR)
            return end;
        if (!JSONReader::GetValue(tmp, value))
            return end;
        
        return JSONIterator(value, false);
    }

    if (IsObject())
    {
        if (!JSONReader::FirstMember(GetReadBuffer(), value))
            return end;
        
        return JSONIterator(value, true);
    }
    
    return end;
}

JSONIterator JSON::Next(JSONIterator it)
{
    JSONIterator    end;
    ReadBuffer      tmp;
    ReadBuffer      value;
    
    if (it.data.GetBuffer() < GetBuffer() || it.data.GetBuffer() >= GetBuffer() + GetLength())
        return end;
    
    if (IsArray())
    {
        tmp = GetReadBuffer();
        tmp.Advance(it.data.GetBuffer() + it.data.GetLength() - GetBuffer());
        if (tmp.GetLength() > 0 && tmp.GetCharAt(0) == ',')
            tmp = JSON_SkipChar(tmp, ',');
        if (tmp.GetLength() > 0 && tmp.GetCharAt(0) == ']')
            return end;

        if (!JSONReader::GetValue(tmp, value))
            return end;

        return JSONIterator(value, false);
    }
    
    if (IsObject())
    {
        tmp = GetReadBuffer();
        tmp.Advance(it.data.GetBuffer() - GetBuffer());
        tmp = JSON_SkipString(tmp);
        tmp = JSON_SkipChar(tmp, ':');
        tmp = JSON_SkipValue(tmp, 1);
        if (tmp.GetLength() > 0 && tmp.GetCharAt(0) == ',')
            tmp = JSON_SkipChar(tmp, ',');
        if (tmp.GetLength() > 0 && tmp.GetCharAt(0) == '}')
            return end;
        
        return JSONIterator(tmp, true);
    }
    
    return end;
}

bool JSON::IsString()
{
    return GetType() == JSON::String;
}

bool JSON::IsNumber()
{
    return GetType() == JSON::Number;
}

bool JSON::IsObject()
{
    return GetType() == JSON::Object;
}

bool JSON::IsArray()
{
    return GetType() == JSON::Array;
}

bool JSON::IsTrue()
{
    return GetType() == JSON::True;
}

bool JSON::IsFalse()
{
    return GetType() == JSON::False;
}

bool JSON::IsNull()
{
    return GetType() == JSON::Null;
}

bool JSON::IsUndefined()
{
    return GetType() == JSON::Undefined;
}

bool JSON::GetMember(ReadBuffer name, JSON& json)
{
    ReadBuffer  value;

    if (GetType() != JSON::Object)
        return false;

    if (!JSONReader::FindValue(GetReadBuffer(), name, value))
        return false;
    
    json.data = value;
    json.SetType(GetJSONType(value));
    
    return true;
}

bool JSON::GetArrayLength(unsigned& length)
{
    ReadBuffer  value;
    
    if (GetType() != JSON::Array)
        return false;
    
    return JSONReader::GetArray(GetReadBuffer(), value, length);
}

bool JSON::GetArrayElement(unsigned index, JSON& json)
{
    ReadBuffer  value;

    if (GetType() != JSON::Array)
        return false;

    if (!JSONReader::GetArrayElement(GetReadBuffer(), index, value))
        return false;
    
    json.data = value;
    json.SetType(GetJSONType(value));
    
    return true;
}

bool JSON::GetStringValue(ReadBuffer& value)
{
    if (GetType() != JSON::String)
        return false;
    
    return JSONReader::GetStringValue(GetReadBuffer(), value);
}

bool JSON::GetInt64Value(int64_t& value)
{
    int64_t     ret;
    ReadBuffer  buffer;

    if (GetType() != JSON::Number)
        return false;

    buffer = data;
    ret = 0;
    while (buffer.GetLength() > 0 && isdigit(buffer.GetCharAt(0)))
    {
        ret = ret * 10 + buffer.GetCharAt(0) - '0';
        buffer.Advance(1);
    }

    value = ret;
    return true;
}

bool JSON::GetBoolValue(bool& value)
{
    JSON::Type  type;

    type = GetType();
    if (type == JSON::False)
    {
        value = false;
        return true;
    }

    if (type == JSON::True)
    {
        value = true;
        return true;
    }

    return false;
}

ReadBuffer JSON::GetReadBuffer()
{
    ReadBuffer  ret;
    
    ret = data;
    ret.SetLength(GetLength());
    
    return ret;
}