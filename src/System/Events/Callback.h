#ifndef CALLBACK_H
#define CALLBACK_H

class Callback
{
public:
	Callback() {}
	
	void operator()() const { func(arg); }
	void Execute() const { func(arg); }

protected:
	Callback(void (*func)(void*), void* arg) : func(func), arg(arg) {}

private:
	void	(*func)(void*);
	void	*arg;
};


class FunctionCallback : public Callback
{
public:
	FunctionCallback(void (*func)(void)) : Callback(Thunk, (void*) func) {}

private:
	static void Thunk(void* arg)
	{
		void (*cfunc)(void) = (void (*)(void)) arg;
		cfunc();
	}
};

template<class Type, void (Type::*Member)()>
class MemberCallback : public Callback
{
public:
	MemberCallback(Type* obj) : Callback(Thunk, obj) {}

private:
	static void Thunk(void* arg)
	{
		Type* obj = (Type*) arg;
		(obj->*Member)();
	}
};

#endif
