package com.example.scheduler.config;

import com.example.scheduler.service.TaskRunner;
import com.example.scheduler.service.TaskEngine;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * RunnerRegistrar: 管理 type -> TaskRunner 的解析与缓存。
 * 优先策略：
 *  1) cache
 *  2) 已注册的 Spring bean（bean name == type）
 *  3) batch.properties 映射（value 可为 beanName 或 fully-qualified class name）
 *  4) 在已注册 bean 中按 simple class name / FQCN 查找
 *
 * 安全：当 properties 指定类名时，默认仅允许白名单包前缀被实例化
 *
 * 支持的 batch.properties 格式：
 *   code.index=com.example.scheduler.runner.CodeIndexRunner
 *   my.legacy=legacyRunnerBeanName
 */
@Slf4j
@Component
public class RunnerRegistrar {

    private final ApplicationContext ctx;
    private final AutowireCapableBeanFactory beanFactory;
    private final TaskEngine engine;

    // all TaskRunner beans available in context: beanName -> instance
    private final Map<String, TaskRunner> runnersByBeanName;

    // cache: type -> TaskRunner
    private final Map<String, TaskRunner> cache = new ConcurrentHashMap<>();

    // properties loaded from classpath:/batch.properties
    private final Properties props = new Properties();

    // allowed package prefixes for instantiating classes from properties
    private final List<String> allowedPackagePrefixes;

    @Autowired
    public RunnerRegistrar(ApplicationContext ctx, AutowireCapableBeanFactory beanFactory, TaskEngine engine) {
        this.ctx = ctx;
        this.beanFactory = beanFactory;
        this.engine = engine;

        Map<String, TaskRunner> beans = ctx.getBeansOfType(TaskRunner.class);
        this.runnersByBeanName = Collections.unmodifiableMap(new HashMap<>(beans));

        String env = System.getProperty("runner.allowed.packages", System.getenv("RUNNER_ALLOWED_PACKAGES"));
        if (env != null && !env.trim().isEmpty()) {
            List<String> list = new ArrayList<>();
            for (String s : env.split(",")) {
                s = s.trim();
                if (!s.isEmpty()) list.add(s);
            }
            this.allowedPackagePrefixes = Collections.unmodifiableList(list);
        } else {
            this.allowedPackagePrefixes = Collections.unmodifiableList(Arrays.asList("com.example", "org.example"));
        }
    }

    @PostConstruct
    public void init() {
        try {
            Resource r = new ClassPathResource("batch.properties");
            if (r.exists()) {
                try (InputStream in = r.getInputStream()) {
                    props.load(in);
                    log.info("Loaded batch.properties ({} entries)", props.size());
                }
            } else {
                log.debug("No batch.properties found on classpath");
            }
        } catch (Exception ex) {
            log.warn("Failed to load batch.properties", ex);
        }

        // 2. beanName -> runner to cache
        for (Map.Entry<String, TaskRunner> e : runnersByBeanName.entrySet()) {
            cache.putIfAbsent(e.getKey(), e.getValue());
        }
        // 3. also cache by simple class name + fqcn
        for (TaskRunner r : runnersByBeanName.values()) {
            cache.putIfAbsent(r.getClass().getSimpleName(), r);
            cache.putIfAbsent(r.getClass().getName(), r);
        }

        log.info("RunnerRegistrar initialized: beanCount={}, cachedTypes={}", runnersByBeanName.size(), cache.keySet().size());

        // 4. Register all discovered types to TaskEngine (try to register; engine handles duplicates)
        Set<String> keysToRegister = new LinkedHashSet<>();
        keysToRegister.addAll(cache.keySet());
        // also add props keys (explicit mapping)
        for (Object k : props.keySet()) keysToRegister.add(String.valueOf(k));

        for (String type : keysToRegister) {
            try {
                Optional<TaskRunner> opt = getRunner(type);
                if (!opt.isPresent()) {
                    // if mapping in props points to a class name, getRunner may instantiate; if still absent -> warn.
                    log.debug("No runner instance found for type '{}', skipping engine registration", type);
                    continue;
                }
                TaskRunner r = opt.get();
                // engine will log/warn on conflict
                engine.register(type, r);
            } catch (Throwable t) {
                log.warn("Failed to register runner for type {} : {}", type, t.toString());
            }
        }
    }

    /**
     * 返回 optional runner
     */
    public Optional<TaskRunner> getRunner(String type) {
        if (type == null) return Optional.empty();

        // 1.cache
        TaskRunner cached = cache.get(type);
        if (cached != null) return Optional.of(cached);

        // 2.beanName match
        TaskRunner beanMatch = runnersByBeanName.get(type);
        if (beanMatch != null) {
            cache.putIfAbsent(type, beanMatch);
            return Optional.of(beanMatch);
        }

        // 3.properties mapping
        String mapped = props.getProperty(type);
        if (mapped != null && !mapped.trim().isEmpty()) {
            mapped = mapped.trim();
            // 3a. mapped is beanName
            if (runnersByBeanName.containsKey(mapped)) {
                TaskRunner b = runnersByBeanName.get(mapped);
                cache.put(type, b);
                return Optional.of(b);
            }
            // 3b. mapped is FQCN -> try to create using spring beanFactory
            try {
                if (!isAllowedClassName(mapped)) {
                    log.warn("Mapped runner class not allowed by package whitelist: {}", mapped);
                } else {
                    Class<?> cls = Class.forName(mapped);
                    if (!TaskRunner.class.isAssignableFrom(cls)) {
                        log.warn("Mapped class does not implement TaskRunner: {}", mapped);
                    } else {
                        // autowire dependencies
                        Object bean = beanFactory.createBean(cls);
                        TaskRunner created = (TaskRunner) bean;
                        cache.put(type, created);
                        cache.putIfAbsent(created.getClass().getSimpleName(), created);
                        cache.putIfAbsent(created.getClass().getName(), created);
                        return Optional.of(created);
                    }
                }
            } catch (Exception e) {
                log.warn("Cannot instantiate mapped runner class: " + mapped, e);
            }
        }

        // 4. try by simple class name / FQCN among existing beans
        for (TaskRunner tr : runnersByBeanName.values()) {
            if (tr.getClass().getSimpleName().equals(type) || tr.getClass().getName().equals(type)) {
                cache.putIfAbsent(type, tr);
                return Optional.of(tr);
            }
        }

        return Optional.empty();
    }

    public boolean hasRunner(String type) {
        return getRunner(type).isPresent();
    }

    /**
     * 返回可用的 runner 类型（仅用于前端显示）。
     * 仅返回 batch.properties 中定义的 key，避免 simpleName / FQCN 干扰 UI。
     */
    public Set<String> availableTypes() {
        if (!props.isEmpty()) {
            Set<String> keys = new LinkedHashSet<>();
            for (Object k : props.keySet()) {
                keys.add(String.valueOf(k));
            }
            return keys;
        }
        // 如果没有 properties，则返回 beanName
        return new LinkedHashSet<>(runnersByBeanName.keySet());
    }

    /**
     * 安全检查：只允许白名单包前缀的类名被实例化（properties 指定类名情况）
     */
    private boolean isAllowedClassName(String fqcn) {
        if (fqcn == null) return false;
        for (String p : allowedPackagePrefixes) {
            if (fqcn.startsWith(p)) return true;
        }
        return false;
    }
}