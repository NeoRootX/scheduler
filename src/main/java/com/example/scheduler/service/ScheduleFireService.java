package com.example.scheduler.service;

import com.example.scheduler.domain.BatchSchedule;
import com.example.scheduler.domain.TaskStatus;
import com.example.scheduler.repo.ScheduleRepo;
import com.example.scheduler.repo.TaskRepo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class ScheduleFireService {
    private final ScheduleRepo scheduleRepo;
    private final TaskRepo taskRepo;
    private final long windowSeconds = 3600;

    @Scheduled(fixedDelay = 10000L, initialDelay = 5000L)
    @Transactional
    public void fireDue() {
        ZoneId zone = ZoneId.systemDefault();
        ZonedDateTime now = ZonedDateTime.now(zone).withNano(0);

        List<BatchSchedule> enabled = scheduleRepo.findByEnabled(1);
        for (BatchSchedule s : enabled) {
            if (!StringUtils.hasText(s.getCron())) continue;
            CronExpression cron;
            try {
                cron = CronExpression.parse(s.getCron());
            } catch (Exception e) {
                log.warn("Invalid cron id={}, cron={}", s.getId(), s.getCron());
                continue;
            }

            ZonedDateTime last = s.getLastFireAt() == null ? now.minusSeconds(windowSeconds) : s.getLastFireAt().toInstant().atZone(zone);
            ZonedDateTime start = last;
            ZonedDateTime end = now;

            List<ZonedDateTime> toFire = new ArrayList<>();
            ZonedDateTime next = cron.next(start.minusSeconds(1));
            while (next != null && !next.isAfter(end)) {
                toFire.add(next);
                next = cron.next(next);
                if (toFire.size() > 5000) break;
            }

            for (ZonedDateTime t : toFire) {
                String ticket = "schedule#" + s.getId() + "#" + t.toLocalDateTime().toString().replace(":", "").replace("-", "");
                int inserted = taskRepo.insertIfNotExists(
                        ticket,                         // 1: ticket_no
                        s.getType(),                    // 2: type
                        s.getPayload(),                 // 3: payload
                        0,                              // 4: priority
                        TaskStatus.PENDING.name(),      // 5: status
                        0,                              // 6: attempts
                        3,                              // 7: max_attempts
                        Timestamp.from(t.toInstant()),  // 8: not_before
                        s.getId()                       // 9: schedule_id
                );
                if (inserted > 0) {
                    log.info("Backfill/Fired schedule id={}, cron={}, at={}", s.getId(), s.getCron(), t);
                    scheduleRepo.updateLastFireAt(s.getId(), Timestamp.from(t.toInstant()));
                }
            }
        }
    }
}