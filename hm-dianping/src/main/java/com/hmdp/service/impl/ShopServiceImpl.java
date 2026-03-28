package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryById(Long id) {
        return queryByIdWithMutex(id);
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        if (x == null || y == null) {
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            return Result.ok(page.getRecords());
        }

        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;

        String key = SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo().radius(
                key,
                new Circle(new Point(x, y), new Distance(5000)),
                RedisGeoCommands.GeoRadiusCommandArgs.newGeoRadiusArgs().includeDistance().limit(end)
        );
        if (results == null) {
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> content = results.getContent();
        if (content.size() <= from) {
            return Result.ok(Collections.emptyList());
        }

        List<Long> ids = new ArrayList<>(content.size());
        Map<String, Distance> distanceMap = new HashMap<>(content.size());
        content.stream().skip(from).forEach(result -> {
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            distanceMap.put(shopIdStr, result.getDistance());
        });

        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query()
                .in("id", ids)
                .last("order by field(id," + idStr + ")")
                .list();
        for (Shop shop : shops) {
            Distance distance = distanceMap.get(shop.getId().toString());
            if (distance != null) {
                shop.setDistance(distance.getValue());
            }
        }
        return Result.ok(shops);
    }

    public Result queryByIdWithMutex(Long id) {
        String key = CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(shopJson)) {
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return Result.ok(shop);
        }
        if (shopJson != null) {
            return Result.fail("店铺不存在！");
        }

        String lockKey = LOCK_SHOP_KEY + id;
        Shop shop;
        boolean isLock = false;
        try {
            isLock = tryLock(lockKey);
            if (!isLock) {
                Thread.sleep(50);
                return queryByIdWithMutex(id);
            }

            // 获取锁后再次检查缓存，避免重复回源数据库
            shopJson = stringRedisTemplate.opsForValue().get(key);
            if (StrUtil.isNotBlank(shopJson)) {
                shop = JSONUtil.toBean(shopJson, Shop.class);
                return Result.ok(shop);
            }
            if (shopJson != null) {
                return Result.fail("店铺不存在！");
            }

            shop = getById(id);
            if (shop == null) {
                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                return Result.fail("店铺不存在！");
            }

            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
            return Result.ok(shop);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Result.fail("查询店铺失败！");
        } finally {
            if (isLock) {
                unlock(lockKey);
            }
        }
    }

    public Result queryByIdWithLogicalExpire(Long id) {
        String key = CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        // 仅在真正未命中（null）时回源数据库并写入逻辑过期数据
        if (shopJson == null) {
            Shop shop = getById(id);
            if (shop == null) {
                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                return Result.fail("店铺不存在！");
            }
            setWithLogicalExpire(key, shop, CACHE_SHOP_TTL, TimeUnit.MINUTES);
            return Result.ok(shop);
        }
        // 命中空值或空白占位（如\t\n），直接返回，避免缓存穿透
        if (StrUtil.isBlank(shopJson)) {
            return Result.fail("店铺不存在！");
        }

        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();

        // 未过期，直接返回
        if (expireTime.isAfter(LocalDateTime.now())) {
            return Result.ok(shop);
        }

        // 过期后尝试获取锁，成功则异步重建；失败直接返回旧数据
        String lockKey = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        if (isLock) {
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    Shop latestShop = getById(id);
                    if (latestShop == null) {
                        stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                        return;
                    }
                    setWithLogicalExpire(key, latestShop, CACHE_SHOP_TTL, TimeUnit.MINUTES);
                } finally {
                    unlock(lockKey);
                }
            });
        }

        return Result.ok(shop);
    }

    private void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    private boolean tryLock(String key) {
        Boolean success = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return Boolean.TRUE.equals(success);
    }

    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }

    @Override
    public Result updateShop(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺id不能为空！");
        }
        boolean success = updateById(shop);
        if (!success) {
            return Result.fail("更新店铺失败！");
        }
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
        return Result.ok();
    }
}
