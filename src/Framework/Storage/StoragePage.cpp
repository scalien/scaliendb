#include "StoragePage.h"

StoragePage::StoragePage()
{
    prev = next = this;
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
