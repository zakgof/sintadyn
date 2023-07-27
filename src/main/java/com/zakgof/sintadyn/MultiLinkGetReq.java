package com.zakgof.sintadyn;

import java.util.List;

public interface MultiLinkGetReq<K1, V1, K2, V2, R> {
    QueryCommand<List<Link<K2, V2, R>>> value(V1 from);
}
