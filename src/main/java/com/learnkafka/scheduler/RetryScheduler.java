package com.learnkafka.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.config.LibraryEventConsumerConfig;
import com.learnkafka.consumer.LibraryEventsConsumer;
import com.learnkafka.entity.FailedRecord;
import com.learnkafka.jpa.FailedRecordRepository;
import com.learnkafka.service.LibraryEventsService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class RetryScheduler {

    private FailedRecordRepository failedRecordRepository;
    private LibraryEventsService libraryEventsService;

    public RetryScheduler(FailedRecordRepository failedRecordRepository, LibraryEventsService libraryEventsService) {
        this.failedRecordRepository = failedRecordRepository;
        this.libraryEventsService = libraryEventsService;
    }

    @Scheduled(fixedRate = 10000)
    public void retryFailedRecords() {
        log.info("Retrying failed records started");
        failedRecordRepository.findAllByStatus(LibraryEventConsumerConfig.RETRY)
                .forEach(failedRecord -> {
                    log.info("Retrying failed records : {} ", failedRecord);
                    var consumerRecord = buildConsumerRecord(failedRecord);
                    try {
                        libraryEventsService.processLibraryEvent(consumerRecord);
                        failedRecord.setStatus(LibraryEventConsumerConfig.SUCCESS);
                        failedRecordRepository.save(failedRecord);
                    } catch (Exception e) {
                        log.error("Exception in retryFailedRecords: {} ", e.getMessage(), e);
                    }
                });
        log.info("Retrying failed records completed");
    }

    private ConsumerRecord<Integer, String> buildConsumerRecord(FailedRecord failedRecord) {

        return new ConsumerRecord<>(
                failedRecord.getTopic(),
                failedRecord.getPartition(),
                failedRecord.getOffset_value(),
                failedRecord.getKey_value(),
                failedRecord.getErrorRecord()
                );
    }
}
