package com.zakgof.sintadyn;

import org.objenesis.Objenesis;
import org.objenesis.ObjenesisStd;
import org.objenesis.instantiator.ObjectInstantiator;
import software.amazon.awssdk.core.SdkBytes;
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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

class SintaType<K, V> implements Type<K, V> {

    private static final Objenesis objenesis = new ObjenesisStd();

    private final List<Field> propertyFields;
    private final Field keyField;
    private final String entity;
    private final ObjectInstantiator<V> objenesisInstantiator;

    public SintaType(Class<V> valueClass) {
        this.propertyFields = getAllFields(valueClass);
        this.keyField = getKeyField(propertyFields);
        this.entity = valueClass.getSimpleName().toLowerCase(Locale.ROOT);
        this.objenesisInstantiator = objenesis.getInstantiatorOf(valueClass);
    }

    private AttributeValue getPropertyValue(Field field, V value) {
        return convertAttribute(getFieldValue(field, value), field.getType());
    }

    private <F> F getFieldValue(Field field, V value) {
        try {
            //noinspection unchecked
            return (F) field.get(value);
        } catch (IllegalAccessException e) {
            throw new SintadynException(e);
        }
    }

    private <F> void setFieldValue(Field field, F fieldValue, V value) {
        try {
            field.set(value, fieldValue);
        } catch (IllegalAccessException e) {
            throw new SintadynException(e);
        }
    }

    private AttributeValue convertAttribute(Object attribute, Class<?> type) {
        if (type == int.class || type == Integer.class
                || type == Byte.class || type == byte.class
                || type == Short.class || type == short.class
                || type == Long.class || type == long.class
                || type == Character.class || type == char.class) {
            return AttributeValue.builder().n(attribute.toString()).build(); // TODO: nulls
        } else if (type == String.class) {
            return AttributeValue.builder().s((String) attribute).build();
        } else if (type == Boolean.class) {
            return AttributeValue.builder().bool((Boolean) attribute).build();
        } else if (type == byte[].class) {
            return AttributeValue.builder().b(SdkBytes.fromByteArray((byte[]) attribute)).build();
        } else throw new SintadynException("Non-mappable attribute type:" + type);
        // TODO : array types
        // TODO : built-in convertors
        // TODO : custom convertors
    }

    private <F> F deconvertAttribute(AttributeValue attributeValue, Class<F> type) {
        if (type == String.class) {
            return type.cast(attributeValue.s());
        }
        throw new SintadynException("Non-mappable attribute type:" + type);
        // TODO: all other types
    }

    static List<Field> getAllFields(Class<?> type) {
        List<Field> fields = new ArrayList<>();
        Field[] declaredFields = type.getDeclaredFields();
        Arrays.sort(declaredFields, Comparator.comparing(Field::getName));
        for (Field field : declaredFields) {
            if ((field.getModifiers() & (Modifier.STATIC | Modifier.TRANSIENT)) == 0) {
                field.setAccessible(true);
                fields.add(field);
            }
        }
        if (type.getSuperclass() != null)
            fields.addAll(getAllFields(type.getSuperclass()));
        return fields;
    }

    private Field getKeyField(List<Field> propertyFields) {
        Set<Field> keyFields = propertyFields.stream()
                .filter(field -> field.getAnnotation(Key.class) != null)
                .collect(Collectors.toSet());
        if (keyFields.isEmpty()) {
            throw new SintadynException("No key field found");
        } else if (keyFields.size() > 1) {
            throw new SintadynException("Multiple key fields found");
        }
        Field keyField = keyFields.iterator().next();
        propertyFields.remove(keyField); // TODO: performance optimization
        return keyField;
    }

    private Map<String, AttributeValue> deflate(Sintadyn sintadyn, V value) {
        Map<String, AttributeValue> map =
                propertyFields.stream()
                        .collect(Collectors.toMap(Field::getName, field -> getPropertyValue(field, value)));
        Map<String, AttributeValue> keyAttrMap = deflateKey(sintadyn, getFieldValue(keyField, value));
        map.putAll(keyAttrMap);
        map.put(sintadyn.entityType(), AttributeValue.fromS(entity));
        return map;
    }

    private Map<String, AttributeValue> deflateKey(Sintadyn sintadyn, K key) {
        Map<String, AttributeValue> map = new HashMap<>();
        // TODO: key must be a string
        // TODO: configurable separator
        AttributeValue keyAttrValue = convertAttribute(entity + "#" + key, String.class);
        map.put(sintadyn.pk(), keyAttrValue);
        map.put(sintadyn.sk(), keyAttrValue);
        return map;
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

    private class SintaPutReq implements PutReq<K, V> {

        @Override
        public ExecCommand value(V value) {
            return (Sintadyn sintadyn, DynamoDbClient dynamoDB) ->
                    dynamoDB.putItem(builder -> builder.tableName(sintadyn.table())
                            .item(deflate(sintadyn, value)));
        }

        @Override
        public ExecCommand values(Collection<V> values) {
            return (Sintadyn sintadyn, DynamoDbClient dynamoDB) -> {
                Map<String, List<WriteRequest>> requestItems = Map.of(sintadyn.table(),
                        values.stream()
                                .map(value -> deflate(sintadyn, value))
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
                        .key(deflateKey(sintadyn, key)));
                return inflate(sintadyn, getItemResponse.item());
            };
        }

        @Override
        public QueryCommand<List<V>> keys(List<K> keys) {
            return (Sintadyn sintadyn, DynamoDbClient dynamoDB) -> {

                List<Map<String, AttributeValue>> deflatedKeys = keys.stream()
                        .map(key -> deflateKey(sintadyn, key))
                        .collect(Collectors.toList());

                Map<String, KeysAndAttributes> requestItems = Map.of(sintadyn.table(), KeysAndAttributes.builder()
                        .keys(deflatedKeys)
                        .build());

                BatchGetItemResponse batchGetItemResponse = dynamoDB.batchGetItem(builder -> builder.requestItems(requestItems));

                // TODO: hasUnprocessedKeys!!!

                return batchGetItemResponse.responses().get(sintadyn.table()).stream()
                        .map(attrMap -> inflate(sintadyn, attrMap))
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
                        .map(attrMap -> inflate(sintadyn, attrMap))
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
                            .key(deflateKey(sintadyn, key)));
        }

        @Override
        public ExecCommand keys(Collection<K> keys) {
            return (Sintadyn sintadyn, DynamoDbClient dynamoDB) -> {
                Map<String, List<WriteRequest>> requestItems = Map.of(sintadyn.table(),
                        keys.stream()
                                .map(key -> deflateKey(sintadyn, key))
                                .map(item -> WriteRequest.builder()
                                        .deleteRequest(drbuilder -> drbuilder.key(item))
                                        .build())
                                .collect(Collectors.toList()));
                dynamoDB.batchWriteItem(bwirbuilder -> bwirbuilder.requestItems(requestItems));
            };
        }
    }

    private V inflate(Sintadyn sintadyn, Map<String, AttributeValue> item) {
        V value = objenesisInstantiator.newInstance();
        propertyFields.forEach(field -> {
            AttributeValue attributeValue = item.get(field.getName());
            Object fieldValue = deconvertAttribute(attributeValue, field.getType());
            setFieldValue(field, fieldValue, value);
        });
        AttributeValue keyAttributeValue = item.get(sintadyn.pk());
        Object keyValue = deconvertAttribute(keyAttributeValue, keyField.getType());
        String keyFieldValue = keyValue.toString().substring(entity.length() + "#".length()); // TODO!!!
        setFieldValue(keyField, keyFieldValue, value);
        return value;
    }


}
