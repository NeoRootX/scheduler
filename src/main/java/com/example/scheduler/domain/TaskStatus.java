package com.example.scheduler.domain;

public enum TaskStatus {
    PENDING,     // 待执行（可领取）
    RUNNING,     // 执行中
    SUCCEED,     // 成功
    FAILED,      // 失败（可人工重试）
    CANCELED,     // 已取消（引擎应跳过）
    CANCEL_REQUESTED // 已请求取消（正在跑，等 Runner/引擎协作停下）
}