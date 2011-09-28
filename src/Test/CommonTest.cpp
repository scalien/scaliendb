#include "Test.h"
#include "System/Common.h"

typedef struct human_bytes_pair {
    uint64_t    value;
    char        text[5];
} human_bytes_pair_t;

TEST_DEFINE(TestCommonHumanBytes)
{
    char                buf[5];
    unsigned            i;
    human_bytes_pair_t  human_byte_pairs[] = {
        {1,             "1"},
        {10,            "10"},
        {100,           "100"},
        {1000,          "1.0k"},
        {10000,         "10k"},
        {100000,        "100k"},
        {1000000,       "1.0M"},
        {10000000,      "10M"},
        {100000000,     "100M"},
        {1000000000,    "1.0G"},
        {10000000000,   "10G"},
        {100000000000,  "100G"},
        {1000000000000, "1.0T"},
        {1234567890123, "1.2T"},
        {999,           "999"},
        {1000,          "1.0k"},
        {1449,          "1.4k"},
        {1450,          "1.5k"},
        {1499,          "1.5k"},        
        {1599,          "1.6k"},
        {1999,          "2.0k"},
    };

    for (i = 0; i < SIZE(human_byte_pairs); i++)
    {
        HumanBytes(human_byte_pairs[i].value, buf);
        TEST_ASSERT(strncmp(buf, human_byte_pairs[i].text, 5) == 0);
        //TEST_LOG("%" PRIu64 " => %s", human_byte_pairs[i].value, buf);
    }
    
    return TEST_SUCCESS;
}

TEST_DEFINE(TestCommonRandomDistribution)
{
    unsigned    num;
    unsigned    i;
    unsigned    count[11];
    unsigned    min;
    unsigned    max;
    unsigned    maxdev;
    int         rnd;
    
    SeedRandom();
    num = 10 * 1000 * 1000;
    
    for (i = 0; i < SIZE(count); i++)
        count[i] = 0;
    
    for (i = 0; i < num; i++)
    {
        rnd = RandomInt(0, SIZE(count) - 1);
        TEST_ASSERT(rnd >= 0 && (unsigned) rnd < SIZE(count));
        count[rnd % SIZE(count)]++;
    }
    
    min = (unsigned) -1;
    max = 0;
    maxdev = 0;

    for (i = 0; i < SIZE(count); i++)
    {
        if (count[i] < min)
            min = count[i];
        if (count[i] > max)
            max = count[i];
        if (count[i] * SIZE(count) > num)
        {
            if (count[i] - (float)num / SIZE(count) > maxdev)
                maxdev = count[i] - (float)num / SIZE(count);
        }
        else
        {
            if ((float)num / SIZE(count) - count[i] > maxdev)
                maxdev = (float)num / SIZE(count) - count[i];
        }
        TEST_LOG("i: % 3d percentage: %2.2f%%", i, (float)count[i] / num * 100.0);
    }
    
    // more than 10% error considered bad discrete uniform distribution
    // the C library random()/rand() is such bad random generator
    if (maxdev > (float)num / SIZE(count) * 0.10)
    {
        TEST_LOG("Error: %2.1f%%", maxdev / ((float)num / SIZE(count) / 100.0));
        TEST_FAIL();
    }
    
    return TEST_SUCCESS;
}

TEST_DEFINE(TestCommonUInt64ToBufferWithBase)
{
    char        valueBuffer[] = "1Hh3t53O16";
    char        buffer[100];
    uint64_t    value = 23274728509509702ULL;
    int         ret;

    ret = UInt64ToBufferWithBase(buffer, sizeof(buffer), value, 64);
    TEST_ASSERT(ret == 10);

    ret = strcmp(valueBuffer, buffer);
    TEST_ASSERT(ret == 0);

    return TEST_SUCCESS;
}
