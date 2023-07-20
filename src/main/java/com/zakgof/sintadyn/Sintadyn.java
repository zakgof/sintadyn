package com.zakgof.sintadyn;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeyType;

public class Sintadyn {

    private final String table;
    private final String pk;
    private final String sk;
    private final String entityType;

    public Sintadyn(String table, String pk, String sk, String entityType) {
        this.table = table;
        this.pk = pk;
        this.sk = sk;
        this.entityType = entityType;
    }

    public static <K, V> Type<K, V> type(Class<V> valueClass) {
        return new SintaType<>(valueClass);
    }

    public static <K1, V1, K2, V2> ManyToMany<K1, V1, K2, V2> manyToMany(Type<K1, V1> fromType, Type<K2, V2> toType) {
        return null;
    }

    public static Sintadyn.Builder builder() {
        return new Sintadyn.Builder();
    }

    public String table() {
        return table;
    }

    public String pk() {
        return pk;
    }

    public String sk() {
        return sk;
    }

    public String entityType() {
        return entityType;
    }


    @SuppressWarnings("unchecked")
    public void createTable(DynamoDbClient dynamoDB) {
        dynamoDB.createTable(builder -> builder.tableName(table)
                .keySchema(ksebuilder -> ksebuilder.attributeName(pk).keyType(KeyType.HASH),
                        ksebuilder -> ksebuilder.attributeName(sk).keyType(KeyType.RANGE)
                )
                .globalSecondaryIndexes(GlobalSecondaryIndex.builder()
                        .indexName(entityType)
                        .keySchema(ksebuilder -> ksebuilder.attributeName(entityType).keyType(KeyType.HASH),
                                ksebuilder -> ksebuilder.attributeName(sk).keyType(KeyType.RANGE))
                        .build())
        );
        // TODO: make entityType GSI optional
    }

    public static class Builder {

        private String table = "sintadyn";
        private String pk = "pk";
        private String sk = "sk";
        private String entityType = "entityType";

        public Builder table(String table) {
            this.table = table;
            return this;
        }

        public Builder pk(String pk) {
            this.pk = pk;
            return this;
        }

        public Builder sk(String sk) {
            this.sk = sk;
            return this;
        }

        public Builder entityType(String entityType) {
            this.entityType = entityType;
            return this;
        }

        public Sintadyn build() {
            return new Sintadyn(table, pk, sk, entityType);
        }
    }
}
