package com.example.scheduler.domain;

import lombok.*;

import javax.persistence.*;
import java.sql.Timestamp;

@Entity
@Getter @Setter @ToString
@Table(name = "batch_task", indexes = {@Index(name = "idx_task_status", columnList = "status"), @Index(name = "idx_task_not_before", columnList = "not_before"), @Index(name = "idx_task_pick", columnList = "status, not_before, priority, id")})
public class BatchTask {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "schedule_id")
    private Long scheduleId;

    @Column(name = "ticket_no", unique = true)
    private String ticketNo;

    @Column(name = "type", nullable = false, length = 64)
    private String type;

    @Lob
    @Column(name = "payload")
    private String payload;

    @Column(name = "priority")
    private Integer priority;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 16)
    private TaskStatus status = TaskStatus.PENDING;

    @Column(name = "attempts")
    private Integer attempts;

    @Column(name = "max_attempts")
    private Integer maxAttempts;

    @Column(name = "not_before", columnDefinition = "TIMESTAMP(3)")
    private Timestamp notBefore;

    @Column(name = "owner", length = 64)
    private String owner;

    @Column(name = "heartbeat_at", columnDefinition = "TIMESTAMP(3)")
    private Timestamp heartbeatAt;

    @Column(name = "created_at", nullable = false, columnDefinition = "TIMESTAMP(3)")
    private Timestamp createdAt;

    @Column(name = "updated_at", nullable = false, columnDefinition = "TIMESTAMP(3)")
    private Timestamp updatedAt;

    @Column(name = "finish_at", columnDefinition = "TIMESTAMP(3)")
    private Timestamp finishAt;

    @Column(name = "message", length = 2000)
    private String message;

    @PrePersist
    public void prePersist() {
        Timestamp now = new Timestamp(System.currentTimeMillis());
        createdAt = now;
        updatedAt = now;
    }

    @PreUpdate
    public void preUpdate() {
        updatedAt = new Timestamp(System.currentTimeMillis());
    }
}