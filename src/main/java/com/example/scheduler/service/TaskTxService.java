package com.example.scheduler.service;

import com.example.scheduler.domain.OperationLog;
import com.example.scheduler.domain.BatchRun;
import com.example.scheduler.domain.BatchTask;
import com.example.scheduler.domain.TaskStatus;
import com.example.scheduler.repo.OperationLogRepo;
import com.example.scheduler.repo.TaskPicker;
import com.example.scheduler.repo.TaskRepo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class TaskTxService {

    private final TaskRepo taskRepo;
    private final OperationLogRepo opRepo;
    private final TaskPicker picker;

    @PersistenceContext
    private EntityManager em;

    /**
     * ThreadLocal run context（方便 Runner 在执行过程中调用 logCompensation 时不用显式传 runId）
     */
    private static final ThreadLocal<Long> RUN_CTX = new ThreadLocal<>();

    public void bindRunContext(Long runId) {
        RUN_CTX.set(runId);
    }

    public void unbindRunContext() {
        RUN_CTX.remove();
    }

    /**
     * 如果 Runner 想隐式使用当前 runId，可调用 currentRunId()
     */
    public Long currentRunId() {
        return RUN_CTX.get();
    }

    /**
     * A. 原子领取（短事务 / 新事务）- 通过 TaskPicker 的 SELECT ... FOR UPDATE SKIP LOCKED + UPDATE 保证原子性
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Optional<BatchTask> claimOneTx(String owner) {
        Optional<Long> idOpt = picker.lockOnePendingId();
        if (!idOpt.isPresent()) return Optional.empty();

        Long id = idOpt.get();
        int ok = picker.markRunning(id, owner);
        if (ok == 0) {
            // 被其它实例抢走
            return Optional.empty();
        }
        BatchTask task = em.find(BatchTask.class, id);
        return Optional.ofNullable(task);
    }

    /**
     * 新建 run 记录（短事务 / 新事务）
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public BatchRun createRunTx(Long taskId, Timestamp startedAt) {
        BatchRun run = new BatchRun();
        run.setTaskId(taskId);
        run.setStatus(com.example.scheduler.domain.RunStatus.RUNNING);
        run.setStartedAt(startedAt);
        em.persist(run);
        return run;
    }

    /**
     * 完成回写（短事务 / 新事务）- 统一在 finally 中调用，finishAt 为真正的结束时刻
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void completeTx(Long taskId, Long runId,
                           boolean succeed, String message, Timestamp finishAt,
                           TaskStatus finalTaskStatus) {
        BatchTask task = em.find(BatchTask.class, taskId);
        if (task == null) {
            log.warn("Task not found when completing, id={}", taskId);
            return;
        }
        BatchRun run = em.find(BatchRun.class, runId);
        if (run == null) {
            // 极端情况兜底（比如 run 行意外缺失）
            run = new BatchRun();
            run.setTaskId(taskId);
            run.setStartedAt(finishAt);
        }

        TaskStatus statusToSet = (finalTaskStatus != null)
                ? finalTaskStatus
                : (succeed ? TaskStatus.SUCCEED : TaskStatus.FAILED);

        task.setStatus(statusToSet);
        task.setMessage(message);
        task.setFinishAt(finishAt);
        Timestamp now = finishAt;
        if (task.getCreatedAt() == null) task.setCreatedAt(now);
        task.setUpdatedAt(now);

        com.example.scheduler.domain.RunStatus runStatus = (statusToSet == TaskStatus.CANCELED)
                ? com.example.scheduler.domain.RunStatus.CANCELED
                : (succeed ? com.example.scheduler.domain.RunStatus.SUCCEED : com.example.scheduler.domain.RunStatus.FAILED);
        run.setStatus(runStatus);
        run.setEndedAt(finishAt);
        run.setMessage(message);

        em.merge(run);
        taskRepo.save(task);
    }

    /**
     * 读取取消标记（短只读事务）
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public boolean isCancelRequestedTx(Long taskId) {
        BatchTask t = em.find(BatchTask.class, taskId);
        return t != null && t.getStatus() == TaskStatus.CANCEL_REQUESTED;
    }

    /**
     * 在当前事务中记录一条补偿动作（调用方应保证在修改之前或之后调用，写入 DB）
     * 如果调用方没有显式传入 runId，可以用 currentRunId() 获取（显式传入以降低隐式依赖）
     */
    @Transactional(propagation = Propagation.REQUIRED)
    public void logCompensation(Long runId, String actionType, String payloadJson) {
        if (runId == null) {
            // 尝试从 ThreadLocal 获取
            runId = currentRunId();
            if (runId == null) {
                throw new IllegalStateException("runId is null for logCompensation");
            }
        }
        Integer maxSeq = opRepo.findMaxSeqNoByRunId(runId);
        int next = (maxSeq == null ? 1 : maxSeq + 1);
        OperationLog op = new OperationLog();
        op.setRunId(runId);
        op.setSeqNo(next);
        op.setActionType(actionType);
        op.setActionPayload(payloadJson);
        op.setStatus("PENDING");
        opRepo.save(op);
    }

    /**
     * 获取某 runId 的补偿项（逆序）
     */
    @Transactional(readOnly = true)
    public List<OperationLog> fetchCompensationsDesc(Long runId) {
        return opRepo.findByRunIdOrderBySeqNoDesc(runId);
    }

    /**
     * 标记补偿项状态
     */
    @Transactional
    public void markCompensationDone(Long opId) {
        OperationLog o = opRepo.findById(opId)
                .orElseThrow(() -> new IllegalStateException("OperationLog not found: " + opId));
        o.setStatus("DONE");
        opRepo.save(o);
    }

    @Transactional
    public void markCompensationFailed(Long opId, String error) {
        OperationLog o = opRepo.findById(opId)
                .orElseThrow(() -> new IllegalStateException("OperationLog not found: " + opId));
        Integer attempts = o.getAttempts();
        o.setAttempts((attempts == null ? 0 : attempts) + 1);
        o.setLastError(error);
        o.setStatus("FAILED");
        opRepo.save(o);
    }
}