package com.example.scheduler.domain;

import lombok.*;

import javax.persistence.*;
import java.sql.Timestamp;

@Entity
@Table(name = "batch_run", indexes = { @Index(name="idx_run_task", columnList = "task_id") })
@Getter @Setter @ToString
public class BatchRun {
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name="task_id", nullable = false)
    private Long taskId;

    @Column(nullable = false, updatable = false)
    private Timestamp startedAt;

    private Timestamp endedAt;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 16)
    private RunStatus status = RunStatus.RUNNING;

    @Column(length = 2000)
    private String message;

    @PrePersist
    public void prePersist() {
        startedAt = new Timestamp(System.currentTimeMillis());
    }
}
