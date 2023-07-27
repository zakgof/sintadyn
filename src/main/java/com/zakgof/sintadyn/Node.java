package com.zakgof.sintadyn;

public interface Node<K, V> {
    PutReq<K, V> put();

    GetReq<K, V> get();

    DeleteReq<K,V> delete();

    K keyOf(V fromValue);
}
