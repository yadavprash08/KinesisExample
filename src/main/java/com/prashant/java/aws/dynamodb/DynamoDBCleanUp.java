package com.prashant.java.aws.dynamodb;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.*;
import com.google.common.util.concurrent.RateLimiter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * Created by yprasha on 4/21/16.
 */
@Slf4j
public class DynamoDBCleanUp {

    private final AmazonDynamoDB dynamoDB;
    private Consumer<String> tableConsumer;

    public DynamoDBCleanUp(AmazonDynamoDB dynamoDB) {
        this.dynamoDB = dynamoDB;
        tableConsumer = this::listTable;
    }

    public void listAllTables() {
        processEachTable(tableConsumer);
    }

    public void listTable(String tableName) {
        log.info("TableName:: {}", tableName);
    }

    public void deleteAllXRegionTables() {
        processEachTable(this::deleteXRegionTable);
    }

    private void deleteXRegionTable(String tableName) {
        if (StringUtils.contains(tableName, "xregion")) {
            deleteTable(tableName);
        }
    }

    public void deleteTable(String tableName) {
        log.info("Deleting table {}", tableName);
        DeleteTableRequest request = new DeleteTableRequest().withTableName(tableName);
        DeleteTableResult result = dynamoDB.deleteTable(request);
        ensureTableDeleted(tableName);
    }

    private void ensureTableDeleted(String tableName) {
        try {
            DescribeTableResult tableDescription = dynamoDB.describeTable(tableName);
            ensureTableDeleted(tableName);
        } catch (ResourceNotFoundException rNFE) {
            return;
        }
    }

    private void processEachTable(Consumer<String> tableConsumer) {
        log.info("Consuming all the tables in the account.");
        ListTablesRequest request = new ListTablesRequest();
        ListTablesResult tables;
        do {
            tables = dynamoDB.listTables(request);
            tables.getTableNames().parallelStream().forEach(t -> tableConsumer.accept(t));
            request = new ListTablesRequest().withExclusiveStartTableName(tables.getLastEvaluatedTableName());
        } while (Objects.nonNull(tables.getLastEvaluatedTableName()));
    }


    public static void main(String[] args) {
        AmazonDynamoDB dynamoDB = new AmazonDynamoDBClient();
        dynamoDB.setRegion(Region.getRegion(Regions.US_EAST_1));

        DynamoDBCleanUp cleanUp = new DynamoDBCleanUp(dynamoDB);
        cleanUp.listAllTables();
        cleanUp.deleteAllXRegionTables();
    }


}
