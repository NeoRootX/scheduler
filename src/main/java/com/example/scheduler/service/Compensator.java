package com.example.scheduler.service;

import com.fasterxml.jackson.databind.JsonNode;

public interface Compensator {
    /**
     * 执行补偿逻辑（应幂等），payload 为 action_payload 的 JsonNode。
     * 返回 true = 成功，false = 失败（需要重试或人工干预）
     */
    boolean compensate(Long runId, JsonNode payload) throws Exception;

    /**
     * 返回此补偿器对应的 actionType
     */
    String actionType();
}