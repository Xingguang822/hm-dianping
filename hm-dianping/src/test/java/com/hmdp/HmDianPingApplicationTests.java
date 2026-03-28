package com.hmdp;

import com.hmdp.entity.Shop;
import com.hmdp.service.IShopService;
import com.hmdp.utils.RedisWorker;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.SHOP_GEO_KEY;

@SpringBootTest
class HmDianPingApplicationTests {

    @Resource
    private RedisWorker redisWorker;

    @Resource
    private IShopService shopService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Test
    void testRedisWorkerNextId() throws InterruptedException {
        int threadCount = 100;
        int idsPerThread = 100;

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        Set<Long> ids = ConcurrentHashMap.newKeySet();

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < idsPerThread; j++) {
                        ids.add(redisWorker.nextId("order"));
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        boolean completed = latch.await(10, TimeUnit.SECONDS);
        executor.shutdown();

        Assertions.assertTrue(completed, "ID generation tasks timed out");
        Assertions.assertEquals(threadCount * idsPerThread, ids.size());
    }

    @Test
    void loadShopData() {
        List<Shop> shops = shopService.list();
        Map<Long, List<Shop>> shopMap = shops.stream().collect(Collectors.groupingBy(Shop::getTypeId));

        for (Map.Entry<Long, List<Shop>> entry : shopMap.entrySet()) {
            Long typeId = entry.getKey();
            String key = SHOP_GEO_KEY + typeId;
            List<RedisGeoCommands.GeoLocation<String>> locations = new ArrayList<>(entry.getValue().size());
            for (Shop shop : entry.getValue()) {
                locations.add(new RedisGeoCommands.GeoLocation<>(
                        shop.getId().toString(),
                        new Point(shop.getX(), shop.getY())
                ));
            }
            stringRedisTemplate.opsForGeo().add(key, locations);
        }
    }
}
