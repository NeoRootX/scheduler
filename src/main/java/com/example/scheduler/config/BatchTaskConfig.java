package com.example.scheduler.config;

import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

@Component
public class BatchTaskConfig {
    private static final String FILE = "batch.properties";
    private final Map<String, String> mapping = new HashMap<>();

    @PostConstruct
    public void load() {
        Properties p = new Properties();
        ClassPathResource r = new ClassPathResource(FILE);
        try (InputStream in = r.getInputStream()) {
            p.load(in);
            for (String name : p.stringPropertyNames()) {
                String val = p.getProperty(name);
                if (val != null) mapping.put(name.trim(), val.trim());
            }
        } catch (IOException e) {
            System.err.println("Warning: cannot load " + FILE + " : " + e.getMessage());
        }
    }

    public Optional<String> getMapping(String taskCode) {
        return Optional.ofNullable(mapping.get(taskCode));
    }

    public Map<String, String> getAllMappings() {
        return Collections.unmodifiableMap(mapping);
    }
}