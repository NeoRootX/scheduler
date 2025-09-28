package com.example.scheduler.repo;

import com.example.scheduler.domain.BatchSchedule;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.util.List;

@Repository
public interface ScheduleRepo extends JpaRepository<BatchSchedule, Long> {
    List<BatchSchedule> findByEnabled(Integer enabled);

    @Modifying
    @Query(value = "UPDATE batch_schedule SET last_fire_at=?2 WHERE id=?1", nativeQuery = true)
    int updateLastFireAt(Long id, Timestamp ts);
}