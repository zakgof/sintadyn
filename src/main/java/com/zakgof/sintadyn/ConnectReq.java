package com.zakgof.sintadyn;

public interface ConnectReq<K1, V1, K2, V2, R> extends ExecCommand {
    ConnectReq<K1, V1, K2, V2, R> fromValue(V1 fromValue);

    ConnectReq<K1, V1, K2, V2, R> fromKey(K1 fromKey);

    ConnectReq<K1, V1, K2, V2, R> toValue(V2 fromValue);

    ConnectReq<K1, V1, K2, V2, R> toKey(K2 fromKey);

    ConnectReq<K1, V1, K2, V2, R> withEdge(R edge);
}
