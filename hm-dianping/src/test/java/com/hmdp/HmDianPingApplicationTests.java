package com.hmdp;

import com.hmdp.utils.RedisWorker;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.util.Set;
import java.util.concurrent.*;

@SpringBootTest
class HmDianPingApplicationTests {

    @Resource
    private RedisWorker redisWorker;

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
}
