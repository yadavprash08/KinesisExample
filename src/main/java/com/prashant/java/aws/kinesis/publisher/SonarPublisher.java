package com.prashant.java.aws.kinesis.publisher;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by yprasha on 4/21/16.
 */
@Slf4j
public class SonarPublisher {

    private static final String STREAM_NAME = "Sonar-Kinesis-Test";

    private final AmazonKinesis amazonKinesis;
    private ExecutorService executorService = Executors.newFixedThreadPool(30);

    public SonarPublisher(AmazonKinesis amazonKinesis) {
        this.amazonKinesis = amazonKinesis;
    }

    public static void main(String[] args) {
        AWSCredentialsProvider credentials = new DefaultAWSCredentialsProviderChain();
        AmazonKinesis kinesis = new AmazonKinesisClient(credentials);
        kinesis.setRegion(Region.getRegion(Regions.US_WEST_2));

        SonarPublisher publisher = new SonarPublisher(kinesis);
        publisher.publishMessage();
    }

    public void publishMessage() {
        for (int i = 0; i < 30; i++) {
            MessagePublisher publisher = new MessagePublisher(executorService, "Message Publisher - " + i);
            executorService.submit(publisher);
        }
    }

    private class MessagePublisher implements Runnable {

        private final ExecutorService executorService;
        private final String threadName;
        private long messageSequence = 0;
        private String messageKey = UUID.randomUUID().toString();

        public MessagePublisher(ExecutorService executorService, String threadName) {
            this.executorService = executorService;
            this.threadName = threadName;
        }

        public void run() {
            try {
                log.info("Publishing message sequence {} from Thread {} ", messageSequence, threadName);
                PutRecordRequest putRecordRequest = new PutRecordRequest();
                ByteBuffer dateBytes = ByteBuffer.wrap(RandomStringUtils.randomAlphanumeric(50).getBytes());
                putRecordRequest.setData(dateBytes);
                putRecordRequest.setPartitionKey(threadName);
                putRecordRequest.setSequenceNumberForOrdering(Objects.toString(messageSequence));
                putRecordRequest.setStreamName(STREAM_NAME);
                amazonKinesis.putRecord(putRecordRequest);
                messageSequence++;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (!executorService.isShutdown()) {
                    executorService.submit(this);
                }
            }
        }
    }

}
