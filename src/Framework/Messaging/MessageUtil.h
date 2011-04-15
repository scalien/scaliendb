#ifndef MESSAGEUTIL_H
#define MESSAGEUTIL_H

#include "System/Platform.h"
#include "System/Buffers/Buffer.h"

#define MESSAGE_READ_SEPARATOR()    \
    read = buffer.Readf(":");       \
    if (read < 1) return false; \
    buffer.Advance(read);

#define MESSAGE_CHECK_ADVANCE(n)    \
    if (read < n)           \
        return false;       \
    buffer.Advance(read)

/*
===============================================================================================

 MessageUtil - template based helper functions for reading/writing lists

===============================================================================================
*/

class MessageUtil
{
public:
    template<typename List>
    static void WriteIDList(List& IDs, Buffer& buffer)
    {
        uint64_t*   it;
        
        // write number of items        
        buffer.Appendf("%u", IDs.GetLength());
        
        FOREACH (it, IDs)
            buffer.Appendf(":%U", *it);
    }

    template<typename List>
    static bool ReadIDList(List& IDs, ReadBuffer& buffer)
    {
        uint64_t    ID;
        unsigned    i, num;
        int         read;
        
        num = 0;
        read = buffer.Readf("%u", &num);
        MESSAGE_CHECK_ADVANCE(1);
        
        for (i = 0; i < num; i++)
        {
            MESSAGE_READ_SEPARATOR();
            read = buffer.Readf("%U", &ID);
            MESSAGE_CHECK_ADVANCE(1);
            // XXX in case of List type is SortedList, a LessThan function
            // implementation needed in the cpp file where ReadIDList is called!
            IDs.Add(ID);
        }
        
        return true;
    }
};

#endif
