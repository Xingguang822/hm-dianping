package com.hmdp.service;

import java.time.LocalDate;

public interface IUvService {

    void recordUv(String biz, Long bizId, String visitorId, LocalDate date);

    long countUv(String biz, Long bizId, LocalDate date);
}

