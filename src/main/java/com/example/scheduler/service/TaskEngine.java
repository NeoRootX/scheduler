package com.example.scheduler.service;

import com.example.scheduler.domain.BatchRun;
import com.example.scheduler.domain.BatchTask;
import com.example.scheduler.domain.OperationLog;
import com.example.scheduler.domain.TaskStatus;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.lang.management.ManagementFactory;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

@Slf4j
@Service
@RequiredArgsConstructor
public class TaskEngine {

    private final TaskTxService tx;                        // 短事务交给 TaskTxService
    private final ObjectMapper mapper;                     // JSON 解析
    private final List<TaskRunner> autoRunners;            // Spring 注入的 Runner beans（可为 empty）
    private final ThreadPoolTaskExecutor taskExec;         // 线程池
    private final CompensatorRegistry compensatorRegistry; // 补偿器注册表

    // key -> TaskRunner
    private final Map<String, TaskRunner> runners = new ConcurrentHashMap<>();

    // 运行中/中断管理
    private final Set<Long> runningIds = ConcurrentHashMap.newKeySet();
    private final Map<Long, Future<?>> futureMap = new ConcurrentHashMap<>();

    // 是否启用严格注册（遇到重复 key 抛异常）。默认 false（宽容模式，避免启动顺序问题）。
    private final boolean strictRegistration = Boolean.parseBoolean(System.getProperty("runner.registration.strict", "false"));

    /**
     * 显式注册
     */
    public void register(String key, TaskRunner r) {
        if (key == null || key.trim().isEmpty()) {
            throw new IllegalArgumentException("TaskRunner key must not be empty");
        }
        if (r == null) {
            throw new IllegalArgumentException("TaskRunner instance must not be null");
        }

        TaskRunner prev = runners.putIfAbsent(key, r);
        if (prev == null) {
            log.info("Runner registered: {} -> {}", key, r.getClass().getName());
            return;
        }

        if (prev == r) {
            log.debug("Register called but same instance already registered for key '{}'", key);
            return;
        }

        String msg = String.format("Duplicate TaskRunner key attempted: '%s' (existing=%s, new=%s)",
                key, prev.getClass().getName(), r.getClass().getName());

        if (strictRegistration) {
            log.error(msg);
            throw new IllegalStateException(msg);
        } else {
            log.warn(msg + " — keeping existing mapping.");
        }
    }

    public void register(TaskRunner r) {
        if (r == null) return;
        String key = r.getClass().getSimpleName();
        log.info("Auto-registering runner by simple class name: {} -> {}", key, r.getClass().getName());
        register(key, r);
    }

    @PostConstruct
    public void autoRegister() {
        if (autoRunners == null || autoRunners.isEmpty()) {
            log.info("No auto runners found in context for autoRegister.");
            return;
        }
        for (TaskRunner r : autoRunners) {
            try {
                register(r);
            } catch (Throwable t) {
                log.warn("Failed to auto-register runner {} : {}", r == null ? "null" : r.getClass().getName(), t.toString());
            }
        }
        log.info("TaskEngine autoRegister finished. Registered keys: {}", runners.keySet());
    }

    /**
     * 对外入口：领取 → 执行业务（线程池）→ 完成回写
     */
    public void pollAndRunOnce() {
        Optional<BatchTask> opt = tx.claimOneTx(owner());
        if (!opt.isPresent()) return;

        BatchTask task = opt.get();
        BatchRun run = tx.createRunTx(task.getId(), tsNow());

        final Long taskId = task.getId();
        final String type = task.getType();
        final String payload = safePayload(task.getPayload());

        log.info("Submit task to pool: id={}, type={}", taskId, type);

        Future<?> f = taskExec.submit(() -> executeAndComplete(taskId, type, payload, run.getId()));
        futureMap.put(taskId, f);
    }

    /**
     * 真正执行体（线程池中运行） - 增加 runContext 绑定与补偿触发
     */
    private void executeAndComplete(Long taskId, String type, String payload, Long runId) {
        runningIds.add(taskId);
        boolean succeed = false;
        String errMsg = null;
        TaskStatus finalStatus = null;

        // 绑定 runId 上下文，方便 Runner 内隐式 logCompensation 使用
        tx.bindRunContext(runId);
        try {
            log.info("Start execute task: id={}, type={}, runId={}", taskId, type, runId);

            if (tx.isCancelRequestedTx(taskId)) {
                finalStatus = TaskStatus.CANCELED;
                errMsg = "Canceled before start";
                return;
            }

            TaskRunner r = runners.get(type);
            if (r == null) throw new IllegalStateException("No runner for type=" + type);

            if (tx.isCancelRequestedTx(taskId)) {
                finalStatus = TaskStatus.CANCELED;
                errMsg = "Canceled right before start";
                return;
            }
            if (Thread.currentThread().isInterrupted()) {
                finalStatus = TaskStatus.CANCELED;
                errMsg = "Interrupted before start";
                return;
            }

            // 执行 Runner（Runner 内部应通过 tx.logCompensation(...) 写补偿项）
            r.initJob(mapper.readTree(payload));
            succeed = true;
            finalStatus = TaskStatus.SUCCEED;

        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.warn("Task interrupted id={}", taskId, ie);
            succeed = false;
            finalStatus = TaskStatus.CANCELED;
            errMsg = "Interrupted during execution";

        } catch (Exception e) {
            log.error("Task failed id={}", taskId, e);
            errMsg = trimErr(e.getMessage());
            if (finalStatus == null) finalStatus = TaskStatus.FAILED;

            // 发生异常时，同步触发补偿（best-effort）
            try {
                compensateRun(runId);
            } catch (Throwable ce) {
                String compErr = trimErr(ce.toString());
                log.error("Compensation process error for run {} : {}", runId, compErr, ce);
                // 将补偿错误附加到 errMsg，便于上报
                errMsg = (errMsg == null ? "" : errMsg + " | ") + "CompensationError: " + compErr;
            }
        } finally {
            try {
                tx.completeTx(taskId, runId, succeed, errMsg, tsNow(), finalStatus);
            } finally {
                runningIds.remove(taskId);
                futureMap.remove(taskId);
                tx.unbindRunContext(); // 解绑 ThreadLocal
            }
        }
    }

    /**
     * 补偿执行：按 OperationLog.seqNo 的逆序（fetchCompensationsDesc 已按 desc 返回）
     * 对每一条 PENDING 的补偿项，调用对应 Compensator 并记录结果。
     */
    private void compensateRun(Long runId) {
        if (runId == null) return;
        log.info("Start compensation for run={}", runId);
        List<OperationLog> ops = tx.fetchCompensationsDesc(runId);
        if (ops == null || ops.isEmpty()) {
            log.info("No compensation entries for run={}", runId);
            return;
        }

        for (OperationLog op : ops) {
            if (op == null) continue;
            Long opId = op.getId();
            String status = op.getStatus();
            if (status == null) status = "";

            // 只处理 PENDING（避免重复处理 DONE/FAILED）
            if (!"PENDING".equalsIgnoreCase(status)) {
                log.debug("Skip compensation opId={} status={}", opId, status);
                continue;
            }

            String actionType = op.getActionType();
            if (actionType == null) {
                tx.markCompensationFailed(opId, "MISSING_ACTION_TYPE");
                continue;
            }

            Compensator c = compensatorRegistry.get(actionType);
            if (c == null) {
                String err = "No compensator registered for actionType=" + actionType;
                log.warn(err);
                tx.markCompensationFailed(opId, err);
                continue;
            }

            try {
                JsonNode payload = mapper.readTree(op.getActionPayload());
                boolean ok = c.compensate(runId, payload);
                if (ok) {
                    tx.markCompensationDone(opId);
                    log.info("Compensation done opId={} action={}", opId, actionType);
                } else {
                    tx.markCompensationFailed(opId, "COMPENSATE_RETURNED_FALSE");
                    log.warn("Compensation returned false opId={} action={}", opId, actionType);
                }
            } catch (Throwable ex) {
                String em = trimErr(ex.toString());
                try {
                    tx.markCompensationFailed(opId, em);
                } catch (Throwable t2) {
                    log.error("Failed to markCompensationFailed for opId={} : {}", opId, t2.toString(), t2);
                }
                log.error("Compensation error opId={} action={} err={}", opId, actionType, em, ex);
                // 继续处理下一条补偿（best-effort）
            }
        }

        log.info("Compensation finished for run={}", runId);
    }

    public boolean isRunning(Long taskId) {
        return runningIds.contains(taskId);
    }

    public boolean interruptIfRunning(Long taskId) {
        Future<?> f = futureMap.get(taskId);
        return f != null && f.cancel(true);
    }

    public TaskRunner getRunner(String key) {
        return key == null ? null : runners.get(key);
    }

    public Set<String> getRunnerTypes() {
        return Collections.unmodifiableSet(new LinkedHashSet<>(runners.keySet()));
    }

    private static String owner() {
        String jvm = ManagementFactory.getRuntimeMXBean().getName();
        String pid = (jvm != null && jvm.contains("@")) ? jvm.substring(0, jvm.indexOf('@')) : "na";
        return "local#" + pid;
    }

    private static Timestamp tsNow() {
        return Timestamp.from(Instant.now());
    }

    private static String safePayload(String payload) {
        return (payload == null || payload.trim().isEmpty()) ? "{}" : payload;
    }

    private static String trimErr(String m) {
        if (m == null) return null;
        m = m.replaceAll("\\s+", " ").trim();
        return m.length() > 1900 ? m.substring(0, 1900) : m;
    }
}