package com.example.scheduler.repo;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.util.List;
import java.util.Optional;

@Repository
@Profile("db2")
public class Db2TaskPicker implements TaskPicker {

    @PersistenceContext
    private EntityManager em;

    /**
     * DB2 领取一条待执行任务：
     * - 仅挑 status='PENDING' 且 not_before 已到期
     * - 行级锁 + 跳过已被其他事务锁定的行
     * - 放在一个短事务中，避免长事务持锁
     */
    @Override
    @Transactional
    @SuppressWarnings("unchecked")
    public Optional<Long> lockOnePendingId() {
        final String sql =
                "SELECT id " +
                        "FROM batch_task " +
                        "WHERE status='PENDING' " +
                        "  AND (not_before IS NULL OR not_before <= CURRENT TIMESTAMP) " +
                        "ORDER BY priority DESC, id ASC " +
                        "FETCH FIRST 1 ROWS ONLY " +
                        "FOR UPDATE WITH RS SKIP LOCKED DATA";

        List<Number> ids = em.createNativeQuery(sql)
                .setMaxResults(1)
                .getResultList();

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
                        "    heartbeat_at=CURRENT TIMESTAMP, updated_at=CURRENT TIMESTAMP " +
                        "WHERE id=:id AND status='PENDING'";

        return em.createNativeQuery(sql)
                .setParameter("owner", owner)
                .setParameter("id", id)
                .executeUpdate();
    }
}