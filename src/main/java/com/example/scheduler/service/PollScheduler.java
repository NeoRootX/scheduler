package com.example.scheduler.service;

import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class PollScheduler {
    private final TaskEngine engine;

    @Scheduled(fixedDelayString = "${scheduler.poll.delay-ms:2000}", initialDelay = 3000L)
    public void tick() {
        int maxPerTick = Integer.getInteger("scheduler.poll.batch", 16);
        for (int i = 0; i < maxPerTick; i++) {
            engine.pollAndRunOnce();
        }
    }
}