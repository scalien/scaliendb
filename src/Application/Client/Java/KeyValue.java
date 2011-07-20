package com.scalien.scaliendb;

import java.util.AbstractMap;

public class KeyValue<K, V> extends AbstractMap.SimpleEntry<K, V>
{
    public KeyValue(K k, V v) {
        super(k, v);
    }
};
