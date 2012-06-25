#include "Watchdog.h"

Watchdog::Watchdog()
{
    thread = NULL;
    running = false;
}

void Watchdog::Start(unsigned timeout_, Callable& callback_)
{
    ASSERT(thread == NULL);

    timeout = timeout_;
    callback = callback_;

    thread = ThreadPool::Create(1);
    thread->Execute(MFUNC(Watchdog, ThreadFunc));

    running = true;
    thread->Start();
}

void Watchdog::Stop()
{
    running = false;
    thread->WaitStop();
    delete thread;
    thread = NULL;
}

void Watchdog::Wake()
{
    flag = true;
}

void Watchdog::ThreadFunc()
{
    while (running)
    {
        flag = false;
        MSleep(timeout);
        if (flag == false)
            Call(callback);
    }
}
