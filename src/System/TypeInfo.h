#ifndef TYPEINFO_H
#define TYPEINFO_H

/*
===============================================================================================

 TypeInfo: simple class for storing type information

===============================================================================================
*/

#ifdef DEBUG
#define TYPEINFO
#endif // DEBUG

class TypeInfo
{
public:
    TypeInfo(const char* type_)
    {
#ifdef TYPEINFO
        type = type_;
#else
        (void) type_;
#endif
    }

    const char* GetType()
    {
#ifdef TYPEINFO
        return type;
#else
        return "";
#endif
    }

#ifdef TYPEINFO
protected:
    const char* type;
#endif
};

#endif
