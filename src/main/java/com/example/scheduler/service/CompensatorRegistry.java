package com.example.scheduler.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class CompensatorRegistry {

    private final Map<String, Compensator> map = new ConcurrentHashMap<>();
    private final List<Compensator> compensators;

    @Autowired
    public CompensatorRegistry(Optional<List<Compensator>> compensators) {
        this.compensators = compensators.orElseGet(Collections::emptyList);
    }

    @PostConstruct
    private void init() {
        for (Compensator c : compensators) {
            try {
                register(c);
            } catch (Exception ex) {
                log.error("Failed to register compensator {} : {}",
                        (c == null ? "null" : c.getClass().getName()), ex.toString());
            }
        }
    }

    public void register(Compensator c) {
        if (c == null) throw new IllegalArgumentException("Compensator must not be null");
        String type = c.actionType();
        if (type == null || type.trim().isEmpty()) {
            throw new IllegalArgumentException("Compensator.actionType() must not be null/empty: " + c.getClass().getName());
        }
        Compensator prev = map.putIfAbsent(type, c);
        if (prev != null && prev != c) {
            log.warn("Compensator conflict for actionType='{}' : existing={} new={}",
                    type, prev.getClass().getName(), c.getClass().getName());
        }
    }

    public Compensator get(String actionType) {
        if (actionType == null) return null;
        return map.get(actionType);
    }

    public boolean contains(String actionType) {
        return actionType != null && map.containsKey(actionType);
    }

    public Set<String> availableTypes() {
        return Collections.unmodifiableSet(new LinkedHashSet<>(map.keySet()));
    }
}