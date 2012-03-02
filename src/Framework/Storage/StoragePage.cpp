#include "StoragePage.h"

StoragePage::StoragePage()
{
    prev = next = this;
    offset = 0;
}

void StoragePage::SetOffset(uint64_t offset_)
{
    offset = offset_;
}

uint64_t StoragePage::GetOffset()
{
    return offset;
}

bool StoragePage::IsCached()
{
    return (prev != this && next != this);
}
