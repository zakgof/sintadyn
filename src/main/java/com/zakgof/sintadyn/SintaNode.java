package com.zakgof.sintadyn;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ComparisonOperator;
import software.amazon.awssdk.services.dynamodb.model.Condition;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

class SintaNode<K, V> implements Node<K, V> {

    private final SintaClass<V> sintaClass;
    private final String entity;

    public SintaNode(Class<V> valueClass) {
        this.sintaClass = new SintaClass<>(valueClass, true);
        this.entity = valueClass.getSimpleName().toLowerCase(Locale.ROOT);
    }

    private Map<String, AttributeValue> deflateWithKey(Sintadyn sintadyn, V value) {
        Map<String, AttributeValue> map = sintaClass.deflateProperties(value);
        K key = keyOf(value);
        map.putAll(deflateKeys(sintadyn, key));
        return map;
    }

    private Map<String, AttributeValue> deflateKeys(Sintadyn sintadyn, K key) {
        AttributeValue keyAttrValue = deflateKey(key);
        return Map.of(
                sintadyn.pk(), keyAttrValue,
                sintadyn.sk(), keyAttrValue
        );
    }

    AttributeValue deflateKey(K key) {
        String keystr = sintaClass.deflateKey(key);
        return AttributeValue.fromS(entity + "#" + keystr);
    }

    @Override
    public K keyOf(V value) {
        return sintaClass.keyOf(value);
    }

    @Override
    public PutReq<K, V> put() {
        return new SintaPutReq();
    }

    @Override
    public GetReq<K, V> get() {
        return new SintaGetReq();
    }

    @Override
    public DeleteReq<K, V> delete() {
        return new SintaDeleteReq();
    }

    String getEntity() {
        return entity;
    }

    Map<String, V> batchQueryByPk(List<String> pks, Sintadyn sintadyn, DynamoDbClient dynamoDB) {

        List<Map<String, AttributeValue>> keys = pks.stream()
                .map(AttributeValue::fromS)
                .map(keyAttrValue -> Map.of(sintadyn.pk(), keyAttrValue, sintadyn.sk(), keyAttrValue))
                .collect(Collectors.toList());

        Map<String, KeysAndAttributes> requestItems = Map.of(sintadyn.table(), KeysAndAttributes.builder()
                .keys(keys)
                .build());

        BatchGetItemResponse batchGetItemResponse = dynamoDB.batchGetItem(builder -> builder.requestItems(requestItems));
        // TODO batchGetItemResponse.hasUnprocessedKeys()

        return batchGetItemResponse.responses().get(sintadyn.table()).stream()
                .collect(Collectors.toMap(
                        attrMap -> attrMap.get(sintadyn.pk()).s(),
                        attrMap -> sintaClass.inflate(sintadyn, attrMap)
                ));
    }

    private class SintaPutReq implements PutReq<K, V> {

        @Override
        public ExecCommand value(V value) {
            return (Sintadyn sintadyn, DynamoDbClient dynamoDB) ->
                    dynamoDB.putItem(builder -> builder.tableName(sintadyn.table())
                            .item(deflateWithKey(sintadyn, value)));
        }

        @Override
        public ExecCommand values(Collection<V> values) {
            return (Sintadyn sintadyn, DynamoDbClient dynamoDB) -> {
                Map<String, List<WriteRequest>> requestItems = Map.of(sintadyn.table(),
                        values.stream()
                                .map(value -> deflateWithKey(sintadyn, value))
                                .map(item -> WriteRequest.builder()
                                        .putRequest(prbuilder -> prbuilder.item(item))
                                        .build())
                                .collect(Collectors.toList()));
                // TODO: make sequential batches of 25 max items
                // TODO: consider batch size in bytes ???
                BatchWriteItemResponse batchWriteItemResponse = dynamoDB.batchWriteItem(builder -> builder.requestItems(requestItems));
                // TODO batchWriteItemResponse.hasUnprocessedItems()
            };
        }
    }

    private class SintaGetReq implements GetReq<K, V> {

        @Override
        public QueryCommand<V> key(K key) {
            return (Sintadyn sintadyn, DynamoDbClient dynamoDB) -> {
                GetItemResponse getItemResponse = dynamoDB.getItem(builder -> builder.tableName(sintadyn.table())
                        .key(deflateKeys(sintadyn, key)));
                return sintaClass.inflate(sintadyn, getItemResponse.item());
            };
        }

        @Override
        public QueryCommand<List<V>> keys(List<K> keys) {
            return (Sintadyn sintadyn, DynamoDbClient dynamoDB) -> {

                List<Map<String, AttributeValue>> deflatedKeys = keys.stream()
                        .map(key -> deflateKeys(sintadyn, key))
                        .collect(Collectors.toList());

                Map<String, KeysAndAttributes> requestItems = Map.of(sintadyn.table(), KeysAndAttributes.builder()
                        .keys(deflatedKeys)
                        .build());

                BatchGetItemResponse batchGetItemResponse = dynamoDB.batchGetItem(builder -> builder.requestItems(requestItems));

                // TODO: hasUnprocessedKeys!!!

                return batchGetItemResponse.responses().get(sintadyn.table()).stream()
                        .map(attrMap -> sintaClass.inflate(sintadyn, attrMap))
                        .collect(Collectors.toList());
            };
        }

        @Override
        public QueryCommand<List<V>> all() {
            return (Sintadyn sintadyn, DynamoDbClient dynamoDB) -> {
                QueryResponse queryResponse = dynamoDB.query(QueryRequest.builder()
                        .tableName(sintadyn.table())
                        .indexName(sintadyn.entityType())
                        .keyConditions(Map.of(sintadyn.entityType(), Condition.builder()
                                .comparisonOperator(ComparisonOperator.EQ)
                                .attributeValueList(AttributeValue.fromS(entity))
                                .build()))
                        .build());
                return queryResponse.items().stream()
                        .map(attrMap -> sintaClass.inflate(sintadyn, attrMap))
                        .collect(Collectors.toList());
                // TODO: hasMore
            };
        }
    }

    private class SintaDeleteReq implements DeleteReq<K, V> {

        @Override
        public ExecCommand key(K key) {
            return (Sintadyn sintadyn, DynamoDbClient dynamoDB) ->
                    dynamoDB.deleteItem(builder -> builder.tableName(sintadyn.table())
                            .key(deflateKeys(sintadyn, key)));
        }

        @Override
        public ExecCommand keys(Collection<K> keys) {
            return (Sintadyn sintadyn, DynamoDbClient dynamoDB) -> {
                Map<String, List<WriteRequest>> requestItems = Map.of(sintadyn.table(),
                        keys.stream()
                                .map(key -> deflateKeys(sintadyn, key))
                                .map(item -> WriteRequest.builder()
                                        .deleteRequest(drbuilder -> drbuilder.key(item))
                                        .build())
                                .collect(Collectors.toList()));
                dynamoDB.batchWriteItem(bwirbuilder -> bwirbuilder.requestItems(requestItems));
            };
        }
    }


}
