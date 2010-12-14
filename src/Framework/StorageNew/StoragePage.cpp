#include "StoragePage.h"

StoragePage::StoragePage()
{
    prev = next = this;
}

void StoragePage::SetOffset(uint64_t offset_)
{
    offset = offset_;
}

uint32_t StoragePage::GetOffset()
{
    return offset;
}
