package com.example.scheduler.repo;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.util.List;
import java.util.Optional;

@Repository
@Profile("mysql")
public class MysqlTaskPicker implements TaskPicker {

    @PersistenceContext
    private EntityManager em;

    /**
     * MySQL 8.0+ 领取一条待执行任务：
     * - 仅挑 status='PENDING' 且 not_before 已到期
     * - SELECT ... FOR UPDATE SKIP LOCKED 实现跳锁
     * - 需在事务中执行（@Transactional 会关闭 autocommit）
     */
    @Override
    @Transactional
    @SuppressWarnings("unchecked")
    public Optional<Long> lockOnePendingId() {
        final String sql =
                "SELECT id " +
                        "FROM batch_task " +
                        "WHERE status='PENDING' " +
                        "  AND (not_before IS NULL OR not_before <= CURRENT_TIMESTAMP(3)) " +
                        "ORDER BY priority DESC, id ASC " +
                        "LIMIT 1 " +
                        "FOR UPDATE SKIP LOCKED";

        List<Number> ids = em.createNativeQuery(sql).getResultList();

        return ids.isEmpty()
                ? Optional.empty()
                : Optional.of(ids.get(0).longValue());
    }

    /**
     * 将任务从 PENDING 原子转为 RUNNING。
     * 只有在当前仍为 PENDING 时更新成功（返回 1）。
     */
    @Override
    @Transactional
    public int markRunning(Long id, String owner) {
        final String sql =
                "UPDATE batch_task " +
                        "SET status='RUNNING', owner=:owner, " +
                        "    heartbeat_at=CURRENT_TIMESTAMP(3), updated_at=CURRENT_TIMESTAMP(3) " +
                        "WHERE id=:id AND status='PENDING'";

        return em.createNativeQuery(sql)
                .setParameter("owner", owner)
                .setParameter("id", id)
                .executeUpdate();
    }
}