#include "UrlParam.h"
#include "System/Buffers/Buffer.h"
#include "System/Platform.h"

#define PARAM_OFFSET(n)     *(int*)(params.GetBuffer() + (n) * sizeof(int) * 2)
#define PARAM_LENGTH(n)     *(int*)(params.GetBuffer() + (n) * sizeof(int) * 2 + sizeof(int))

/*
 * UrlParam class makes url-decoding on the url and hold the parameters
 * in an internal buffer (named 'buffer') as zero terminated strings.
 *
 * The 'params' variable is used as an array for storing parameter offset and
 * length as ints, see macros PARAM_OFFSET and PARAM_LENGTH.
 *
 * The parameters can either be accessed by number or by name. When accessed
 * by name, it finds it by matching the string 'name=' on every parameter.
 */

UrlParam::UrlParam()
{
    numParams = 0;
}

bool UrlParam::Init(const char* url_, int len_, char sep_)
{
    url = url_;
    len = len_;
    sep = sep_;

    numParams = 0;
    buffer.SetLength(0);
    params.SetLength(0);
    
    return Parse();
}

int UrlParam::GetNumParams()
{
    return numParams;
}

const char* UrlParam::GetParam(int nparam)
{
    int offset;
    
    if (nparam >= numParams || nparam < 0)
        return NULL;
    
    offset = PARAM_OFFSET(nparam);
    return &buffer.GetBuffer()[offset];
}

int UrlParam::GetParamLen(int nparam)
{
    int length;
    
    if (nparam >= numParams || nparam < 0)
        return 0;
    
    length = PARAM_LENGTH(nparam);
    return length;
}

int UrlParam::GetParamIndex(const char* name, int namelen)
{
    int     offset;
    int     length;
    char*   param;
    
    if (namelen < 0)
        namelen = (int) strlen(name);
    
    for (int i = 0; i < numParams; i++)
    {
        offset = PARAM_OFFSET(i);
        length = PARAM_LENGTH(i);

        param = &buffer.GetBuffer()[offset];
        if (length > namelen && 
            strncasecmp(name, param, namelen) == 0 &&
            param[namelen] == '=')
        {
            return i;
        }
    }
    
    return -1;
}

bool UrlParam::GetNamed(const char* name, int namelen, ReadBuffer& bs)
{
    int         n;
    
    n = GetParamIndex(name, namelen);
    if (n < 0)
        return false;
    
    bs.SetBuffer((char*) GetParam(n) + (namelen + 1));
    bs.SetLength(GetParamLen(n) - (namelen + 1));

    return true;
}

bool UrlParam::Parse()
{
    const char* s;
    const char* start;
    
    buffer.Allocate(len + 1, false);
    
    s = start = url;
    while (s - url < len)
    {
        if (*s == sep)
        {
            AddParam(start, (int) (s - start));
            start = s + 1;
        }
        s++;
    }
    
    if (s - start > 0)
        AddParam(start, (int) (s - start));
    
    return true;
}

void UrlParam::AddParam(const char *s, int length)
{
    int     offset;
    int     i;
    int     unlen; // length of the unencoded string
    char    c;

    unlen = 0;
    offset = buffer.GetLength();

    for (i = 0; i < length; /* i increased in loop */)
    {
        if (s[i] == '%')
        {
            c = DecodeChar(s + i);
            i += 3;
        }
        else 
        {
            c = s[i];
            i += 1;
        }

        unlen += 1;
        buffer.Append(&c, 1);
    }
    buffer.NullTerminate();
    
    params.Append((const char*) &offset, sizeof(int));
    params.Append((const char*) &unlen, sizeof(int));
    numParams++;
    
}

char UrlParam::DecodeChar(const char* s)
{
    if (s[0] != '%')
        return s[0];
    
    return HexToChar(s[1], s[2]);
}

char UrlParam::HexToChar(char uhex, char lhex)
{
    return (char)(((unsigned char) HexdigitToChar(uhex) * 16) + HexdigitToChar(lhex));
}

char UrlParam::HexdigitToChar(char hexdigit)
{
    if (hexdigit >= '0' && hexdigit <= '9')
        return hexdigit - (char) '0';
    if (hexdigit >= 'a' && hexdigit <= 'f')
        return hexdigit - (char) 'a' + 10;
    if (hexdigit >= 'A' && hexdigit <= 'F')
        return hexdigit - (char) 'A' + 10;
    
    return 0;
}
