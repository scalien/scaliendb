#ifndef CALLABLE_H
#define CALLABLE_H

#include "System/Common.h"
#include "System/Buffers/Buffer.h"
#include <stdlib.h>

#define MFUNC_NAME2(T, Func) T "::" Func "()"
#define MFUNC_NAME(T, Func) MFUNC_NAME2(#T, #Func)
#define MFUNC(T, Func) MFunc<T, &T::Func>(this, MFUNC_NAME2(#T, #Func))
#define MFUNC_OF(T, Func, obj) MFunc<T, &T::Func>(obj, MFUNC_NAME2(#T, #Func))

/*
===============================================================================================

 Callable

===============================================================================================
*/

class Callable
{
public:
    Callable() : func(NULL), arg(NULL), typeName("") {}
    
    void Execute()      { if (!func) return; func(arg); }
    bool IsSet()        { return func != NULL; }
    void Unset()        { func = NULL; arg = NULL; }

    bool operator==(const Callable& other) { return (func == other.func && arg == other.arg); }

    const char* GetType() { return typeName; }

protected:
    Callable(void (*func_)(void*), void* arg_) : func(func_), arg(arg_) {}
    const char* typeName;

private:
    void    (*func)(void*);
    void    *arg;
};

inline void Call(Callable& callable)
{
    callable.Execute();
}

/*
===============================================================================================

 CFunc

===============================================================================================
*/

class CFunc : public Callable
{
public:
    CFunc(void (*func_)(void)) : Callable(Thunk, (void*) func_) {}

private:
    static void Thunk(void* arg)
    {
        void (*cfunc)(void) = (void (*)(void)) arg;
        cfunc();
    }
};

/*
===============================================================================================

 MFunc

===============================================================================================
*/

template<class Type, void (Type::*Member)()>
class MFunc : public Callable
{
public:
    MFunc(Type* obj, const char* name) : Callable(Thunk, obj)
    {
        typeName = name;
    }

private:
    static void Thunk(void* arg)
    {
        Type* obj = (Type*) arg;
        (obj->*Member)();
    }
};

#endif
