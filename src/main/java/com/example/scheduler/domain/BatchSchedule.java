package com.example.scheduler.domain;

import lombok.*;

import javax.persistence.*;
import java.sql.Timestamp;

@Entity
@Table(name = "batch_schedule", indexes = {@Index(name = "idx_sched_enabled", columnList = "enabled")})
@Getter @Setter @ToString
public class BatchSchedule {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(length = 64, nullable = false)
    private String type;

    @Column(length = 64, nullable = false)
    private String cron;

    @Lob
    @Column(name = "payload")
    private String payload;

    @Column(name = "enabled", nullable = false)
    private Integer enabled = 1;

    private Timestamp lastFireAt;
}