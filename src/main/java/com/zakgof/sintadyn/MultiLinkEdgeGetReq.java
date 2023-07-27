package com.zakgof.sintadyn;

import java.util.List;

public interface MultiLinkEdgeGetReq<K1, V1, K2, V2, R> {
    QueryCommand<List<R>> value(V1 fromValue);
}
