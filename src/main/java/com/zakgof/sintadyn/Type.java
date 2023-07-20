package com.zakgof.sintadyn;

public interface Type<K, V> {
    PutReq<K, V> put();

    GetReq<K, V> get();

    DeleteReq<K,V> delete();
}
