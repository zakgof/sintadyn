package com.zakgof.sintadyn;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class MockDynamoDbClient implements DynamoDbClient {

    private final Map<String, MockDynamoDbTable> tables = new LinkedHashMap<>();

    @Override
    public String serviceName() {
        return "MockDynamoDbClient";
    }

    @Override
    public void close() {
    }

    public String toString() {
        return tables.entrySet().stream()
                .map(entry -> entry.getKey() + ": " + entry.getValue().toString())
                .collect(Collectors.joining("||"));
    }

    public MockDynamoDbTable table(String tableName) {
        return tables.get(tableName);
    }

    @Override
    public CreateTableResponse createTable(CreateTableRequest createTableRequest) throws AwsServiceException, SdkClientException {
        String tableName = createTableRequest.tableName();
        String pk = getTableKey(createTableRequest, KeyType.HASH)
                .orElseThrow(() -> new IllegalArgumentException("PK required"));
        String sk = getTableKey(createTableRequest, KeyType.RANGE)
                .orElse(null);
        MockDynamoDbTable table = new MockDynamoDbTable(pk, sk);
        tables.put(tableName, table);
        return CreateTableResponse.builder().build();
    }

    private Optional<String> getTableKey(CreateTableRequest createTableRequest, KeyType keyType) {
        return createTableRequest.keySchema().stream()
                .filter(kse -> kse.keyType() == keyType)
                .findFirst()
                .map(KeySchemaElement::attributeName);
    }

    @Override
    public PutItemResponse putItem(PutItemRequest putItemRequest) throws AwsServiceException, SdkClientException {
        String tableName = putItemRequest.tableName();
        MockDynamoDbTable table = tables.get(tableName);
        Map<String, AttributeValue> item = putItemRequest.item();
        table.put(item);
        return PutItemResponse.builder().build();
    }

    @Override
    public BatchWriteItemResponse batchWriteItem(BatchWriteItemRequest batchWriteItemRequest) throws AwsServiceException, SdkClientException {
        batchWriteItemRequest.requestItems().forEach((tableName, writeItems) -> {
            MockDynamoDbTable table = table(tableName);
            writeItems.forEach(writeItem -> {
                if (writeItem.putRequest() != null) {
                    table.put(writeItem.putRequest().item());
                }
                if (writeItem.deleteRequest() != null) {
                    Map<String, AttributeValue> key = writeItem.deleteRequest().key();
                    AttributeValue pkValue = key.get(table.getPk());
                    AttributeValue skValue = key.get(table.getSk());
                    table.delete(pkValue, skValue);
                }
            });
        });
        return BatchWriteItemResponse.builder().build();
    }

    @Override
    public GetItemResponse getItem(GetItemRequest getItemRequest) throws AwsServiceException, SdkClientException {

        Map<String, AttributeValue> key = getItemRequest.key();
        MockDynamoDbTable table = table(getItemRequest.tableName());
        AttributeValue pkValue = key.get(table.getPk());
        AttributeValue skValue = key.get(table.getSk());

        Map<String, AttributeValue> item = table.get(pkValue, skValue);

        return GetItemResponse.builder()
                .item(item)
                .build();
    }

    @Override
    public BatchGetItemResponse batchGetItem(BatchGetItemRequest batchGetItemRequest) throws AwsServiceException, SdkClientException {
        Map<String, List<Map<String, AttributeValue>>> responses = batchGetItemRequest.requestItems().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> fetch(entry.getKey(), entry.getValue())));
        return BatchGetItemResponse.builder()
                .responses(responses)
                .build();
    }

    private List<Map<String, AttributeValue>> fetch(String tableName, KeysAndAttributes keys) {
        MockDynamoDbTable table = table(tableName);
        return table.batchGet(keys);
    }

    @Override
    public DeleteItemResponse deleteItem(DeleteItemRequest deleteItemRequest) throws AwsServiceException, SdkClientException {
        Map<String, AttributeValue> key = deleteItemRequest.key();
        MockDynamoDbTable table = table(deleteItemRequest.tableName());
        AttributeValue pkValue = key.get(table.getPk());
        AttributeValue skValue = key.get(table.getSk());
        table.delete(pkValue, skValue);
        return DeleteItemResponse.builder().build();
    }
}
