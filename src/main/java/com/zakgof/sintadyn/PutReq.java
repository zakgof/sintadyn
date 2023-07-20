package com.zakgof.sintadyn;

import java.util.Collection;

public interface PutReq<K, V> {
    ExecCommand value(V value);

    ExecCommand values(Collection<V> values);
}
