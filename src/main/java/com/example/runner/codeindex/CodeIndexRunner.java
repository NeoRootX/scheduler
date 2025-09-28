package com.example.runner.codeindex;

import com.example.scheduler.service.TaskRunner;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Paths;
import java.util.List;

@Slf4j
@RequiredArgsConstructor
public class CodeIndexRunner implements TaskRunner {

    private final CodeIndexService codeIndexService;

    @Override
    public void initJob(JsonNode payload) throws Exception {
        log.info("CodeIndexRunner.start payload: {}", payload);

        String root = getRequired(payload, "root");
        String out = getRequired(payload, "output");

        List<String> includes = codeIndexService.readStringArray(payload, "includes");
        List<String> excludes = codeIndexService.readStringArray(payload, "excludes");
        List<String> classpath = codeIndexService.readStringArray(payload, "classpath");

        try {
            codeIndexService.jobProcess(Paths.get(root), Paths.get(out), includes, excludes, classpath);
            log.info("CodeIndexRunner.done root={} output={}", root, out);
        } catch (Exception ex) {
            log.error("CodeIndexRunner.failed root={} output={}", root, out, ex);
            // 上层 TaskEngine 处理重试/补偿/状态变更
            throw ex;
        }
    }

    private String getRequired(JsonNode p, String key) {
        if (p == null || !p.hasNonNull(key)) {
            throw new IllegalArgumentException("payload." + key + " required");
        }
        return p.get(key).asText();
    }
}