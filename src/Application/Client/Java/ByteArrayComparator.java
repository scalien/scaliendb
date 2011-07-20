package com.scalien.scaliendb;

public class ByteArrayComparator implements java.util.Comparator<byte[]>
{
    public int compare(byte[] o1, byte[] o2)
    {
        int len = Math.min(o1.length, o2.length);
        for (int i = 0; i < len; i++)
        {
            if (o1[i] > o2[i])
                return 1;
            else if (o1[i] < o2[i])
                return -1;
        }
        if (o1.length > o2.length)
            return 1;
        else if (o1.length < o2.length)
            return -1;
        else
            return 0;
    }
    
    public boolean equals(byte[] o1, byte[] o2)
    {
        return compare(o1, o2) == 0;
    }
};
