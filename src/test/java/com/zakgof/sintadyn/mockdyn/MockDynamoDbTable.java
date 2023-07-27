package com.zakgof.sintadyn.mockdyn;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MockDynamoDbTable {

    private final List<Map<String, AttributeValue>> records = new ArrayList<>();
    private final String pk;
    private final String sk;

    public MockDynamoDbTable(String pk, String sk) {
        this.pk = pk;
        this.sk = sk;
        if (pk == null) {
            throw new IllegalArgumentException("PK is required");
        }
    }

    public String getPk() {
        return pk;
    }

    public String getSk() {
        return sk;
    }

    public void put(Map<String, AttributeValue> item) {
        records.removeIf(rec -> rec.get(pk).equals(item.get(pk)) && rec.get(sk).equals(item.get(sk)));
        records.add(item);
    }

    @Override
    public String toString() {
        return "pk=" + pk + " sk=" + sk + "\n" +
                records.stream()
                        .map(Object::toString)
                        .collect(Collectors.joining("\n"));
    }

    public List<Map<String, AttributeValue>> batchGet(KeysAndAttributes keyAndAttrs) {
        return keyAndAttrs.keys().stream()
                .flatMap(key -> findBy(key.get(pk), key.get(sk)).stream())
                .collect(Collectors.toList());
    }

    private List<Map<String, AttributeValue>> findBy(AttributeValue pkval, AttributeValue skval) {
        return records.stream()
                .filter(rec -> rec.get(pk).equals(pkval) && rec.get(sk).equals(skval))
                .collect(Collectors.toList());
    }

    public Map<String, AttributeValue> get(AttributeValue pkValue, AttributeValue skValue) {
        return records.stream()
                .filter(rec -> rec.get(pk).equals(pkValue) && rec.get(sk).equals(skValue))
                .findFirst()
                .get();
    }

    public void delete(AttributeValue pkValue, AttributeValue skValue) {
        records.removeIf(rec -> rec.get(pk).equals(pkValue) && rec.get(sk).equals(skValue));
    }
}
