package com.hmdp.controller;

import cn.hutool.core.util.StrUtil;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.service.IUvService;
import com.hmdp.utils.UserHolder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.time.LocalDate;

@RestController
@RequestMapping("/uv")
public class UvController {

    @Resource
    private IUvService uvService;

    /**
     * 记录一次访问，使用 HyperLogLog 去重统计 UV。
     */
    @PostMapping("/{biz}/{id}")
    public Result recordUv(@PathVariable("biz") String biz,
                           @PathVariable("id") Long bizId,
                           HttpServletRequest request) {
        String visitorId = resolveVisitorId(request);
        uvService.recordUv(biz, bizId, visitorId, LocalDate.now());
        return Result.ok();
    }

    /**
     * 查询某业务对象当天 UV。
     */
    @GetMapping("/{biz}/{id}/count")
    public Result countUv(@PathVariable("biz") String biz,
                          @PathVariable("id") Long bizId,
                          @RequestParam(value = "date", required = false) String date) {
        LocalDate targetDate = StrUtil.isBlank(date) ? LocalDate.now() : LocalDate.parse(date);
        long uv = uvService.countUv(biz, bizId, targetDate);
        return Result.ok(uv);
    }

    private String resolveVisitorId(HttpServletRequest request) {
        // 已登录用户优先使用用户 id，保证多端访问去重准确。
        UserDTO user = UserHolder.getUser();
        if (user != null) {
            return "u:" + user.getId();
        }
        // 未登录用户退化为 IP + User-Agent 指纹。
        String ip = request.getRemoteAddr();
        String ua = request.getHeader("User-Agent");
        return "g:" + ip + ":" + (ua == null ? "" : ua);
    }
}

