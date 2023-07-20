package com.zakgof.sintadyn;

import java.util.List;

public interface GetReq<K, V> {
    QueryCommand<V> key(K key);

    QueryCommand<List<V>> keys(List<K> strings);

    QueryCommand<List<V>> all();
}
