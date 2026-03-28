package com.hmdp.service.impl;

import com.hmdp.service.IUvService;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import static com.hmdp.utils.RedisConstants.UV_KEY_PREFIX;

@Service
public class UvServiceImpl implements IUvService {

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public void recordUv(String biz, Long bizId, String visitorId, LocalDate date) {
        String key = buildKey(biz, bizId, date);
        stringRedisTemplate.opsForHyperLogLog().add(key, visitorId);
    }

    @Override
    public long countUv(String biz, Long bizId, LocalDate date) {
        String key = buildKey(biz, bizId, date);
        return stringRedisTemplate.opsForHyperLogLog().size(key);
    }

    private String buildKey(String biz, Long bizId, LocalDate date) {
        return UV_KEY_PREFIX + biz + ":" + bizId + ":" + date.format(DATE_FORMATTER);
    }
}


