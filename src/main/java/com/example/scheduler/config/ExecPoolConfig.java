package com.example.scheduler.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * 通用 Runner 线程池（IO 型任务友好）
 * - 有限队列 + CallerRunsPolicy 形成背压，防止堆积
 * - 核心线程可超时回收，空闲时更省资源
 */
@Configuration
public class ExecPoolConfig {

    @Bean("taskExec")
    public ThreadPoolTaskExecutor taskExec() {
        ThreadPoolTaskExecutor e = new ThreadPoolTaskExecutor();

        // 根据机器核数给出较激进的并发（IO 阻塞型任务友好）
        int cores = Runtime.getRuntime().availableProcessors();
        int corePoolSize = Math.max(16, cores * 8);
        int maxPoolSize = Math.max(32, cores * 16);

        e.setCorePoolSize(corePoolSize);          // 核心线程数
        e.setMaxPoolSize(maxPoolSize);            // 最大线程数
        e.setQueueCapacity(0);                    // 有限队列，避免无限堆积
        e.setKeepAliveSeconds(30);                // 空闲线程回收时间
        e.setAllowCoreThreadTimeOut(true);        // 核心线程也允许回收（低负载省资源）
        e.setThreadNamePrefix("runner-");         // 便于日志排查
        e.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        // 当池子+队列都满时，提交任务的线程自己执行，形成自然限流与背压

        // 优雅关闭
        e.setWaitForTasksToCompleteOnShutdown(true);
        e.setAwaitTerminationSeconds(20);

        e.initialize();

        // 预热：预启动所有核心线程，能提升首次吞吐
        ThreadPoolExecutor tp = e.getThreadPoolExecutor();
        if (tp != null) {
            tp.prestartAllCoreThreads();
        }

        return e;
    }
}