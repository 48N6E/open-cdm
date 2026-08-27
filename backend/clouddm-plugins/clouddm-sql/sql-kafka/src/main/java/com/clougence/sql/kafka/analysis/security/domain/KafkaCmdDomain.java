/*
 * Copyright 2026 杭州开云集致科技有限公司
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package com.clougence.sql.kafka.analysis.security.domain;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.clougence.clouddm.sdk.service.secrules.RuleDomain;
import com.clougence.clouddm.sdk.sql.analysis.behavior.TargetType;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class KafkaCmdDomain extends RuleDomain {

    private String schema;
    private String command;
    private String topic;

    public KafkaCmdDomain(String command) {
        this.command = command;
    }

    @Override
    public List<Map<TargetType, String>> resolveResource() {
        List<Map<TargetType, String>> res = new ArrayList<>();
        HashMap<TargetType, String> map = new HashMap<>();
        map.put(TargetType.Schema, this.schema);
        if (this.topic != null) {
            map.put(TargetType.Table, this.topic);
        }
        res.add(map);
        return res;
    }
}
