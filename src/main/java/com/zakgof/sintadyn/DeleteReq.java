package com.zakgof.sintadyn;

import java.util.Collection;

public interface DeleteReq<K, V> {
    ExecCommand key(K key);

    ExecCommand keys(Collection<K> keys);
}
