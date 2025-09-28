package com.example.runner.compensation;

import com.example.scheduler.service.Compensator;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.file.*;
import java.util.Base64;

@Slf4j
@Component
public class FileRestoreCompensator implements Compensator {

    @Value("${scheduler.default-root:/}")
    private String defaultRoot;

    private static final int MAX_BASE64_LEN = 200 * 1024;

    @Override
    public String actionType() {
        return "file.restore";
    }

    @Override
    public boolean compensate(Long runId, JsonNode payload) throws Exception {
        if (payload == null) {
            log.warn("file.restore: empty payload for run={}", runId);
            return false;
        }

        // 1. 选 root
        Path rootPath;
        if (payload.has("root") && payload.get("root").isTextual()) {
            rootPath = Paths.get(payload.get("root").asText()).toAbsolutePath().normalize();
        } else {
            rootPath = Paths.get(defaultRoot).toAbsolutePath().normalize();
        }

        // 2. file 字段检查
        if (!payload.has("file") || !payload.get("file").isTextual()) {
            log.warn("file.restore: missing 'file' in payload for run={}", runId);
            return false;
        }
        String fileRel = payload.get("file").asText();
        Path target = rootPath.resolve(fileRel).normalize();

        // 3. 安全校验：确保 target 在 rootPath 下，防止路径穿越
        if (!target.startsWith(rootPath)) {
            String msg = String.format("file.restore: illegal target path (outside root) run=%s target=%s root=%s",
                    runId, target, rootPath);
            log.error(msg);
            throw new IllegalArgumentException(msg);
        }

        try {
            if (payload.has("origBase64") && !payload.get("origBase64").isNull()) {
                String b64 = payload.get("origBase64").asText();
                if (b64.length() > MAX_BASE64_LEN) {
                    String msg = String.format("file.restore: origBase64 too large run=%s size=%d", runId, b64.length());
                    log.error(msg);
                    throw new IllegalArgumentException(msg);
                }
                byte[] data = Base64.getDecoder().decode(b64);

                // 确保目录存在
                Files.createDirectories(target.getParent());

                // 原子写入：写临时文件再移动
                Path tmp = Files.createTempFile(target.getParent(), ".tmp-", ".part");
                try {
                    Files.write(tmp, data, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                    // 尝试原子替换
                    try {
                        Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                    } catch (AtomicMoveNotSupportedException amnse) {
                        log.debug("Atomic move not supported for {}, falling back to simple move", target);
                        Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
                    }
                } finally {
                    // 若 tmp 还存在（move 失败或 exception），尝试删除
                    try { if (Files.exists(tmp)) Files.deleteIfExists(tmp); } catch (Exception ignore) {}
                }

                log.info("file.restore: restored file for run={} -> {}", runId, target);
                return true;
            } else {
                // 没有 origBase64，表示原来不存在：删除目标（幂等）
                boolean deleted = Files.deleteIfExists(target);
                log.info("file.restore: deleteIfExists run={} target={} deleted={}", runId, target, deleted);
                return true;
            }
        } catch (Exception ex) {
            log.error("file.restore: failed run={} target={} err={}", runId, target, ex.toString());
            throw ex;
        }
    }
}