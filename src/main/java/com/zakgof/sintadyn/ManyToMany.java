package com.zakgof.sintadyn;

public interface ManyToMany<K1, V1, K2, V2, R> {
    ConnectReq<K1, V1, K2, V2, R> connect();

    MultiLinkGetReq<K1, V1, K2, V2, R> get();

    MultiLinkEdgeGetReq<K1, V1, K2, V2, R> getEdges();
}
