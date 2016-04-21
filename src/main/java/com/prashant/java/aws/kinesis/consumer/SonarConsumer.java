package com.prashant.java.aws.kinesis.consumer;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;
import lombok.extern.slf4j.Slf4j;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by yprasha on 4/21/16.
 */
@Slf4j
public class SonarConsumer {

    private static final String STREAM_NAME = "Sonar-Kinesis-Test";
    public static DefaultAWSCredentialsProviderChain credentialsProvider = new DefaultAWSCredentialsProviderChain();

    public static void main(String[] args) {
        SonarConsumer consumer = new SonarConsumer();
        consumer.consumeMessages("App1");
    }

    public void consumeMessages(String appName) {

        final KinesisClientLibConfiguration kclConfig = new KinesisClientLibConfiguration(appName, STREAM_NAME, credentialsProvider, UUID.randomUUID().toString()).withRegionName(Regions.US_WEST_2.getName()).withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);
        final IRecordProcessorFactory recordProcessorFactory = new RecordProcessorFactory();
        final Worker worker = new Worker.Builder().recordProcessorFactory(recordProcessorFactory).config(kclConfig).build();
        worker.run();
    }

    public class RecordProcessor implements IRecordProcessor {

        private final String processorId;
        private AtomicInteger atomicCounter = new AtomicInteger(0);
        private String shardId;

        public RecordProcessor() {
            processorId = UUID.randomUUID().toString();
            log.info("Created the Record Processor :: {} ", processorId);
        }

        public void initialize(InitializationInput initializationInput) {
            this.shardId = initializationInput.getShardId();
            log.info("Initializing the Shard {}", shardId);
        }

        public void processRecords(ProcessRecordsInput processRecordsInput) {
            processRecordsInput.getRecords().subList(0, 10).stream().forEach(this::consumeRecord);
            checkpoint(processRecordsInput.getCheckpointer());
            if (atomicCounter.get() > 9) {
                throw new RuntimeException("Consumed max eliments for the batch");
            }
        }

        @Override
        public void shutdown(ShutdownInput shutdownInput) {
            IRecordProcessorCheckpointer checkpointer = shutdownInput.getCheckpointer();
            checkpoint(checkpointer);
        }

        private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
            try {
                checkpointer.checkpoint();
            } catch (InvalidStateException e) {
                throw new RuntimeException(e);
            } catch (ShutdownException e) {
                throw new RuntimeException(e);
            }
        }

        private void consumeRecord(Record record) {
            atomicCounter.incrementAndGet();
            log.info("Consuming the Record Partition Key: {}, Data: {}", record.getPartitionKey(), record.getData());
        }
    }

    public class RecordProcessorFactory implements IRecordProcessorFactory {

        @Override
        public IRecordProcessor createProcessor() {
            return new RecordProcessor();
        }
    }
}
