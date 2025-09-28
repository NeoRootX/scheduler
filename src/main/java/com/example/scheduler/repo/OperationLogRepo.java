package com.example.scheduler.repo;

import com.example.scheduler.domain.OperationLog;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface OperationLogRepo extends JpaRepository<OperationLog, Long> {

    List<OperationLog> findByRunIdOrderBySeqNoDesc(Long runId);

    @Query("select max(o.seqNo) from OperationLog o where o.runId = ?1")
    Integer findMaxSeqNoByRunId(Long runId);
}