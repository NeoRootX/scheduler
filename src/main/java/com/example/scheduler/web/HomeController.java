package com.example.scheduler.web;

import com.example.scheduler.config.RunnerRegistrar;
import com.example.scheduler.domain.BatchSchedule;
import com.example.scheduler.domain.BatchTask;
import com.example.scheduler.domain.TaskStatus;
import com.example.scheduler.repo.ScheduleRepo;
import com.example.scheduler.repo.TaskRepo;
import com.example.scheduler.service.TaskRunner;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.sql.Timestamp;
import java.util.Optional;

@Controller
@RequiredArgsConstructor
@Slf4j
public class HomeController {
    private final ScheduleRepo scheduleRepo;
    private final TaskRepo taskRepo;
    private final RunnerRegistrar runnerRegistrar;
    private final ObjectMapper mapper;

    @GetMapping("/")
    public String home(Model model, @RequestParam(value = "ok", required = false) Boolean ok,
                       @RequestParam(value = "type", required = false) String lastType,
                       @RequestParam(value = "payload", required = false) String lastPayload,
                       @RequestParam(value = "cost", required = false) Long costMs,
                       @RequestParam(value = "error", required = false) String error) {
        model.addAttribute("schedules", scheduleRepo.findAll());
        model.addAttribute("tasks", taskRepo.findAll());
        model.addAttribute("runners", runnerRegistrar.availableTypes());
        if (ok != null) {
            model.addAttribute("ok", ok);
            model.addAttribute("lastType", lastType);
            model.addAttribute("lastPayload", lastPayload);
            model.addAttribute("costMs", costMs);
            model.addAttribute("error", error);
        }
        return "home";
    }

    @PostMapping("/manual/run")
    public String manualRun(@RequestParam String type, @RequestParam(required = false) String payload) {
        String json = (payload == null || payload.trim().isEmpty()) ? "{}" : payload.trim();
        long start = System.currentTimeMillis();
        String error = null;

        Optional<TaskRunner> opt = runnerRegistrar.getRunner(type);
        if (!opt.isPresent()) {
            error = "IllegalArgumentException: No runner for type=" + type;
        } else {
            JsonNode node = null;
            try {
                node = mapper.readTree(json);
            } catch (Exception e) {
                error = "BadPayload: " + safeMsg(e.getMessage());
            }

            if (error == null) {
                try {
                    opt.get().initJob(node);
                } catch (Exception e) {
                    log.warn("Runner execution failed: type={}, err={}", type, e.toString());
                    error = e.getClass().getSimpleName() + ": " + safeMsg(e.getMessage());
                }
            }
        }

        long cost = System.currentTimeMillis() - start;
        String redirect = "/?ok=" + (error == null) + "&type=" + urlEncode(type) + "&payload=" + urlEncode(json) + "&cost=" + cost + (error == null ? "" : "&error=" + urlEncode(error));
        return "redirect:" + redirect;
    }

    @PostMapping("/schedules")
    public String create(@ModelAttribute @Validated BatchSchedule s) {
        if (!runnerRegistrar.hasRunner(s.getType())) {
            return "redirect:/?ok=false&error=" + urlEncode("Unknown type: " + s.getType() + ". 请先实现对应 TaskRunner 并注册。");
        }

        String json = (s.getPayload() == null || s.getPayload().trim().isEmpty()) ? "{}" : s.getPayload().trim();
        try {
            mapper.readTree(json);
        } catch (Exception e) {
            return "redirect:/?ok=false&error=" + urlEncode("BadPayload in schedule: " + safeMsg(e.getMessage()));
        }
        s.setPayload(json);
        if (s.getEnabled() == null) s.setEnabled(1);
        scheduleRepo.save(s);
        return "redirect:/?ok=true&type=" + urlEncode(s.getType()) + "&payload=" + urlEncode(json);
    }

    @PostMapping("/tasks/enqueue")
    public String enqueue(@RequestParam String type,
                          @RequestParam(required = false) String payload,
                          @RequestParam(required = false, name = "notBefore") String notBefore) {
        if (!runnerRegistrar.hasRunner(type)) {
            String json = (payload == null || payload.trim().isEmpty()) ? "{}" : payload.trim();
            return "redirect:/?ok=false&type=" + urlEncode(type) + "&payload=" + urlEncode(json) + "&error=" + urlEncode("Unknown type: " + type + "（対応する TaskRunner がありません）");
        }

        String json = (payload == null || payload.trim().isEmpty()) ? "{}" : payload.trim();
        try {
            mapper.readTree(json);
        } catch (Exception e) {
            return "redirect:/?ok=false&type=" + urlEncode(type) + "&payload=" + urlEncode(json) + "&error=" + urlEncode("BadPayload: " + safeMsg(e.getMessage()));
        }

        java.sql.Timestamp nb = null;
        if (notBefore != null && !notBefore.trim().isEmpty()) {
            try {
                String s = notBefore.trim().replace('T', ' ');
                if (s.length() == 16) s = s + ":00";
                if (s.length() >= 19) s = s.substring(0, 19);
                nb = java.sql.Timestamp.valueOf(s);
            } catch (Exception e) {
                return "redirect:/?ok=false&type=" + urlEncode(type) + "&payload=" + urlEncode(json) + "&error=" + urlEncode("notBefore の形式が不正です。例：2025-09-22 08:00:00 または 2025-09-22T08:00");
            }
        }

        BatchTask t = new BatchTask();
        t.setType(type);
        t.setPayload(json);
        t.setStatus(TaskStatus.PENDING);
        t.setScheduleId(null);
        t.setNotBefore(nb);
        taskRepo.save(t);

        return "redirect:/?ok=true&type=" + urlEncode(type) + "&payload=" + urlEncode(json);
    }

    @PostMapping("/schedule/{id}/toggle")
    public String toggleSchedule(@PathVariable Long id, @RequestParam boolean enabled) {
        return scheduleRepo.findById(id).map(s -> {
            s.setEnabled(enabled ? 1 : 0);
            scheduleRepo.save(s);
            return "redirect:/?ok=true";
        }).orElse("redirect:/?ok=false&error=" + urlEncode("Schedule not found: id=" + id));
    }

    @PostMapping("/schedule/{id}/delete")
    public String deleteSchedule(@PathVariable Long id) {
        try {
            return scheduleRepo.findById(id).map(s -> {
                long total = taskRepo.countByScheduleId(id);
                if (total > 0) {
                    String msg = "このスケジュールに紐づくタスクが " + total + " 件あります。先にタスクを削除してください。";
                    return "redirect:/?ok=false&error=" + urlEncode(msg);
                }
                scheduleRepo.deleteById(id);
                return "redirect:/?ok=true&info=" + urlEncode("スケジュールを削除しました: id=" + id);
            }).orElse("redirect:/?ok=false&error=" + urlEncode("Schedule not found: id=" + id));
        } catch (org.springframework.dao.DataIntegrityViolationException ex) {
            String msg = "関連タスクが残っているため削除できません（外部キー制約）。タスクを削除してから再実行してください。";
            return "redirect:/?ok=false&error=" + urlEncode(msg);
        } catch (Exception ex) {
            return "redirect:/?ok=false&error=" + urlEncode("削除中にエラーが発生しました: " + safeMsg(ex.getMessage()));
        }
    }

    @PostMapping("/tasks/{id}/delete")
    public String deleteTask(@PathVariable Long id) {
        return taskRepo.findById(id).map(t -> {
            TaskStatus st = t.getStatus();

            if (st == TaskStatus.RUNNING || st == TaskStatus.CANCEL_REQUESTED) {
                return "redirect:/?ok=false&error=" + urlEncode("実行中/キャンセル待ちのタスクは削除できません: id=" + id);
            }

            taskRepo.deleteById(id);
            return "redirect:/?ok=true&info=" + urlEncode("タスクを削除しました: id=" + id);
        }).orElse("redirect:/?ok=false&error=" + urlEncode("Task not found: id=" + id));
    }

    @PostMapping("/tasks/{id}/cancel")
    public String cancelTask(@PathVariable Long id) {
        return taskRepo.findById(id).map(t -> {
            TaskStatus st = t.getStatus();

            if (st == TaskStatus.PENDING) {
                t.setStatus(TaskStatus.CANCELED);
                t.setUpdatedAt(new Timestamp(System.currentTimeMillis()));
                taskRepo.save(t);
                return "redirect:/?ok=true&info=" + urlEncode("タスクをキャンセルしました: id=" + id);
            }

            if (st == TaskStatus.RUNNING) {
                t.setStatus(TaskStatus.CANCEL_REQUESTED);
                t.setUpdatedAt(new Timestamp(System.currentTimeMillis()));
                taskRepo.save(t);
                return "redirect:/?ok=true&info=" + urlEncode("実行中タスクにキャンセル要求を出しました: id=" + id);
            }

            return "redirect:/?ok=true&info=" + urlEncode("キャンセル不要の状態です: id=" + id + ", status=" + st);
        }).orElse("redirect:/?ok=false&error=" + urlEncode("Task not found: id=" + id));
    }

    private static String safeMsg(String msg) {
        if (msg == null) return "";
        msg = msg.replaceAll("\\s+", " ").trim();
        return msg.length() > 500 ? msg.substring(0, 500) + "..." : msg;
    }

    private static String urlEncode(String s) {
        try {
            return java.net.URLEncoder.encode(s, "UTF-8");
        } catch (Exception e) {
            return "";
        }
    }
}