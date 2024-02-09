package com.learnkafka.service;

import com.learnkafka.entity.FailedRecord;
import com.learnkafka.jpa.FailedRecordRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class FailureService {

    private FailedRecordRepository failedRecordRepository;

    public FailureService(FailedRecordRepository failedRecordRepository) {
        this.failedRecordRepository = failedRecordRepository;
    }

    public void saveFailedRecord(ConsumerRecord<Integer,String> record, Exception exception, String status) {

        var failureRecord = new FailedRecord(null,record.topic(), record.key(),  record.value(), record.partition(),record.offset(),
                exception.getCause().getMessage(),
                status);

        failedRecordRepository.save(failureRecord);

    }
}
