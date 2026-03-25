package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.CACHE_NULL_TTL;
import static com.hmdp.utils.RedisConstants.LOCK_SHOP_TTL;

@Component
@Slf4j
public class CacheClient {

	private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

	@Resource
	private StringRedisTemplate stringRedisTemplate;

	public void set(String key, Object value, Long time, TimeUnit unit) {
		stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
	}

	public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
		RedisData redisData = new RedisData();
		redisData.setData(value);
		redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
		stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
	}

	public <R, ID> R queryWithPassThrough(
			String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
		String key = keyPrefix + id;
		String json = stringRedisTemplate.opsForValue().get(key);

		if (StrUtil.isNotBlank(json)) {
			return JSONUtil.toBean(json, type);
		}
		if (json != null) {
			return null;
		}

		R r = dbFallback.apply(id);
		if (r == null) {
			stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
			return null;
		}

		this.set(key, r, time, unit);
		return r;
	}

	public <R, ID> R queryWithMutex(
			String keyPrefix,
			String lockKeyPrefix,
			ID id,
			Class<R> type,
			Function<ID, R> dbFallback,
			Long time,
			TimeUnit unit) {
		String key = keyPrefix + id;
		String json = stringRedisTemplate.opsForValue().get(key);

		if (StrUtil.isNotBlank(json)) {
			return JSONUtil.toBean(json, type);
		}
		if (json != null) {
			return null;
		}

		String lockKey = lockKeyPrefix + id;
		boolean isLock = false;
		try {
			isLock = tryLock(lockKey);
			if (!isLock) {
				Thread.sleep(50);
				return queryWithMutex(keyPrefix, lockKeyPrefix, id, type, dbFallback, time, unit);
			}

			// 拿到锁后再次检查缓存，避免重复回源
			json = stringRedisTemplate.opsForValue().get(key);
			if (StrUtil.isNotBlank(json)) {
				return JSONUtil.toBean(json, type);
			}
			if (json != null) {
				return null;
			}

			R r = dbFallback.apply(id);
			if (r == null) {
				stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
				return null;
			}

			this.set(key, r, time, unit);
			return r;
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			log.error("互斥锁查询被中断, key={}", key, e);
			return null;
		} finally {
			if (isLock) {
				unlock(lockKey);
			}
		}
	}

	public <R, ID> R queryWithLogicalExpire(
			String keyPrefix,
			String lockKeyPrefix,
			ID id,
			Class<R> type,
			Function<ID, R> dbFallback,
			Long time,
			TimeUnit unit) {
		String key = keyPrefix + id;
		String json = stringRedisTemplate.opsForValue().get(key);

		if (json == null) {
			return null;
		}
		if (StrUtil.isBlank(json)) {
			return null;
		}

		RedisData redisData = JSONUtil.toBean(json, RedisData.class);
		R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
		LocalDateTime expireTime = redisData.getExpireTime();

		if (expireTime != null && expireTime.isAfter(LocalDateTime.now())) {
			return r;
		}

		String lockKey = lockKeyPrefix + id;
		boolean isLock = tryLock(lockKey);
		if (isLock) {
			CACHE_REBUILD_EXECUTOR.submit(() -> {
				try {
					R fresh = dbFallback.apply(id);
					if (fresh == null) {
						stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
						return;
					}
					this.setWithLogicalExpire(key, fresh, time, unit);
				} catch (Exception e) {
					log.error("缓存重建异常, key={}", key, e);
				} finally {
					unlock(lockKey);
				}
			});
		}

		return r;
	}

	private boolean tryLock(String key) {
		Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.SECONDS);
		return BooleanUtil.isTrue(flag);
	}

	private void unlock(String key) {
		stringRedisTemplate.delete(key);
	}
}
