package com.zakgof.sintadyn;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class SintaManyToMany<K1, V1, K2, V2, R> implements ManyToMany<K1, V1, K2, V2, R> {

    private final SintaNode<K1, V1> fromNode;
    private final SintaNode<K2, V2> toNode;
    private final SintaClass<R> sintaEdgeClass;

    SintaManyToMany(Node<K1, V1> fromNode, Node<K2, V2> toNode, Class<R> edgeClass) {
        this.fromNode = (SintaNode<K1, V1>) fromNode;
        this.toNode = (SintaNode<K2, V2>) toNode;
        this.sintaEdgeClass = new SintaClass<>(edgeClass, false);
    }

    @Override
    public ConnectReq<K1, V1, K2, V2, R> connect() {
        return new SintaConnectReq();
    }

    @Override
    public MultiLinkGetReq<K1, V1, K2, V2, R> get() {
        return new SintaMultiLinkGetReq();
    }

    @Override
    public MultiLinkEdgeGetReq<K1, V1, K2, V2, R> getEdges() {
        return new SintaMultiLinkEdgeGetReq();
    }

    private class SintaConnectReq implements ConnectReq<K1, V1, K2, V2, R> {

        private K1 fromKey;
        private K2 toKey;
        private R edge;

        @Override
        public ConnectReq<K1, V1, K2, V2, R> fromValue(V1 fromValue) {
            if (fromKey != null) {
                throw new SintadynException("'from' node is already assigned");
            }
            this.fromKey = fromNode.keyOf(fromValue);
            return this;
        }

        @Override
        public ConnectReq<K1, V1, K2, V2, R> fromKey(K1 fromKey) {
            if (fromKey != null) {
                throw new SintadynException("'from' node is already assigned");
            }
            this.fromKey = fromKey;
            return this;
        }

        @Override
        public ConnectReq<K1, V1, K2, V2, R> toValue(V2 toValue) {
            if (toKey != null) {
                throw new SintadynException("'to' node is already assigned");
            }
            this.toKey = toNode.keyOf(toValue);
            return this;
        }

        @Override
        public ConnectReq<K1, V1, K2, V2, R> toKey(K2 toKey) {
            if (toKey != null) {
                throw new SintadynException("'to' node is already assigned");
            }
            this.toKey = toKey;
            return this;
        }

        @Override
        public ConnectReq<K1, V1, K2, V2, R> withEdge(R edge) {
            if (this.edge != null) {
                throw new SintadynException("edge is already assigned");
            }
            this.edge = edge;
            return this;
        }

        @Override
        public void execute(Sintadyn sintadyn, DynamoDbClient dynamoDB) {
            if (fromKey == null) {
                throw new SintadynException("'from' node is required");
            }
            Map<String, AttributeValue> item = sintaEdgeClass.deflateProperties(edge); // TODO: rules for deflating edges !!!!
            AttributeValue pk = fromNode.deflateKey(fromKey);
            AttributeValue sk = toKey == null ? null :toNode.deflateKey(toKey);
            item.put(sintadyn.pk(), pk);
            item.put(sintadyn.sk(), sk);


            dynamoDB.putItem(builder -> builder.tableName(sintadyn.table())
                    .item(item));
        }
    }

    private QueryResponse queryForLinks(V1 fromValue, Sintadyn sintadyn, DynamoDbClient dynamoDB) {
        K1 fromKey = fromNode.keyOf(fromValue);
        AttributeValue keyAttr = fromNode.deflateKey(fromKey);
        AttributeValue prefixAttr = AttributeValue.fromS(toNode.getEntity() + "#"); // TODO

        QueryResponse queryResponse = dynamoDB.query(qrbuilder -> qrbuilder
                .tableName(sintadyn.table())
                .keyConditionExpression(sintadyn.pk() + "=:pk and begins_with(" + sintadyn.sk() + ", :prefix)")
                .expressionAttributeValues(Map.of(":pk", keyAttr, ":prefix", prefixAttr)));
        return queryResponse;
    }

    private class SintaMultiLinkGetReq implements MultiLinkGetReq<K1, V1, K2, V2, R> {

        @Override
        public QueryCommand<List<Link<K2, V2, R>>> value(V1 fromValue) {
            return (Sintadyn sintadyn, DynamoDbClient dynamoDB) -> {

                QueryResponse queryResponse = queryForLinks(fromValue, sintadyn, dynamoDB);

                List<String> pks = queryResponse.items().stream()
                        .map(item -> item.get(sintadyn.sk()).s())
                        .collect(Collectors.toList());

                Map<String, V2> pkToV2 = toNode.batchQueryByPk(pks, sintadyn, dynamoDB);

                return queryResponse.items().stream()
                        .flatMap(item -> Stream.of(pkToV2.get(item.get(sintadyn.sk()).s()))
                                .map(node -> new SintaLink<>(
                                        toNode.keyOf(node),
                                        node,
                                        sintaEdgeClass.inflate(sintadyn, item)
                                )))
                        .collect(Collectors.toList());
            };
        }
    }

    private class SintaMultiLinkEdgeGetReq implements MultiLinkEdgeGetReq<K1, V1, K2, V2, R> {

        @Override
        public QueryCommand<List<R>> value(V1 fromValue) {
            return (Sintadyn sintadyn, DynamoDbClient dynamoDB) ->
                    queryForLinks(fromValue, sintadyn, dynamoDB).items()
                            .stream()
                            .map(item -> sintaEdgeClass.inflate(sintadyn, item))
                            .collect(Collectors.toList());
        }
    }

    private static class SintaLink<K2, V2, R> implements Link<K2, V2, R> {

        private final K2 key;
        private final V2 node;
        private final R edge;

        private SintaLink(K2 key, V2 node, R edge) {
            this.key = key;
            this.node = node;
            this.edge = edge;
        }

        @Override
        public K2 key() {
            return key;
        }

        @Override
        public V2 node() {
            return node;
        }

        @Override
        public R edge() {
            return edge;
        }
    }
}
