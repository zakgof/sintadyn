package com.zakgof.sintadyn;

public interface Link<K2, V2, R> {
    K2 key();
    V2 node();
    R edge();
}
