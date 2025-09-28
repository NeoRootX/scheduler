package com.example.scheduler.repo;

import com.example.scheduler.domain.BatchTask;
import com.example.scheduler.domain.TaskStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.Optional;

@Repository
public interface TaskRepo extends JpaRepository<BatchTask, Long> {
    Optional<BatchTask> findTopByStatusAndNotBeforeLessThanEqualOrderByPriorityDescIdAsc(String status, Timestamp notBefore);

    @Modifying
    @Query(value =
            "INSERT INTO batch_task(" +
                    "  ticket_no, type, payload, priority, status, attempts, max_attempts, not_before, schedule_id" +
                    ") " +
                    "SELECT ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9 " +
                    "WHERE NOT EXISTS (SELECT 1 FROM batch_task WHERE ticket_no = ?1)",
            nativeQuery = true)
    int insertIfNotExists(
            String ticketNo,
            String type,
            String payload,
            int priority,
            String status,
            int attempts,
            int maxAttempts,
            Timestamp notBefore,
            Long scheduleId
    );

    boolean existsByScheduleId(Long scheduleId);

    long countByScheduleId(Long scheduleId);

    long countByScheduleIdAndStatusIn(Long scheduleId, Collection<String> statuses);

    long countByTypeAndScheduleIdIsNullAndStatusIn(String type, Collection<String> statuses);
}