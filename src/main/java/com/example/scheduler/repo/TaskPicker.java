package com.example.scheduler.repo;

import java.util.Optional;

public interface TaskPicker {
    Optional<Long> lockOnePendingId();

    int markRunning(Long id, String owner);
}