package com.learnkafka.jpa;

import com.learnkafka.entity.FailedRecord;
import org.springframework.data.repository.CrudRepository;

import java.util.List;

public interface FailedRecordRepository extends CrudRepository<FailedRecord, Integer> {

    List<FailedRecord> findAllByStatus(String retry);
}
