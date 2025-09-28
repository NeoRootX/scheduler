package com.example.scheduler.service;

import com.fasterxml.jackson.databind.JsonNode;

public interface TaskRunner {
    void initJob(JsonNode payload) throws Exception;
}