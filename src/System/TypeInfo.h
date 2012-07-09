#ifndef TYPEINFO_H
#define TYPEINFO_H

/*
===============================================================================================

 TypeInfo: simple class for storing type information

===============================================================================================
*/

class TypeInfo
{
public:
    TypeInfo(const char* type_)
    {
        type = type_;
    }

    const char* GetType()
    {
        return type;
    }

protected:
    const char* type;
};

#endif
