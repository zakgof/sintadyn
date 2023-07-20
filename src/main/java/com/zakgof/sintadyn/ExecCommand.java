package com.zakgof.sintadyn;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public interface ExecCommand {
    void execute(Sintadyn syntadyn, DynamoDbClient dynamoDB);
}
