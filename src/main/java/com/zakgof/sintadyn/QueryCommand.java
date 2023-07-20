package com.zakgof.sintadyn;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public interface QueryCommand<T> {
    T execute(Sintadyn syntadyn, DynamoDbClient dynamoDB);
}
