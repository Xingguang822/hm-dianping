package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisWorker;
import com.hmdp.utils.UserHolder;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import lombok.extern.slf4j.Slf4j;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

	@Resource
	private ISeckillVoucherService seckillVoucherService;

	@Resource
	private RedisWorker redisWorker;

	@Resource
	private StringRedisTemplate stringRedisTemplate;

	@Resource
	private RedissonClient redissonClient;

	@Resource
	private ApplicationContext applicationContext;

	private static final String QUEUE_NAME = "stream.orders";
	private static final String GROUP_NAME = "g1";
	private final String consumerName = "c-" + UUID.randomUUID();

	private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
	static {
		SECKILL_SCRIPT = new DefaultRedisScript<>();
		SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
		SECKILL_SCRIPT.setResultType(Long.class);
	}

	private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();
	private IVoucherOrderService proxy;

	@PostConstruct
	private void init() {
		initStream();
		proxy = applicationContext.getBean(IVoucherOrderService.class);
		SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
	}

	private void initStream() {
		// Ensure stream key exists so consumer group can be created at startup.
		RecordId initRecordId = stringRedisTemplate.opsForStream().add(
				StreamRecords.mapBacked(Collections.singletonMap("init", "0")).withStreamKey(QUEUE_NAME)
		);
		try {
			stringRedisTemplate.opsForStream().createGroup(QUEUE_NAME, ReadOffset.latest(), GROUP_NAME);
		} catch (Exception e) {
			if (e.getMessage() == null || !e.getMessage().contains("BUSYGROUP")) {
				log.error("创建stream消费者组失败", e);
			}
		} finally {
			if (initRecordId != null) {
				stringRedisTemplate.opsForStream().delete(QUEUE_NAME, initRecordId);
			}
		}
	}

	private class VoucherOrderHandler implements Runnable {
		@Override
		public void run() {
			while (true) {
				try {
					List<MapRecord<String, Object, Object>> records = stringRedisTemplate.opsForStream().read(
							Consumer.from(GROUP_NAME, consumerName),
							StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
							StreamOffset.create(QUEUE_NAME, ReadOffset.lastConsumed())
					);
					if (records == null || records.isEmpty()) {
						continue;
					}

					MapRecord<String, Object, Object> record = records.get(0);
					Map<Object, Object> value = record.getValue();
					VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
					handleVoucherOrder(voucherOrder);
					stringRedisTemplate.opsForStream().acknowledge(QUEUE_NAME, GROUP_NAME, record.getId());
				} catch (Exception e) {
					log.error("处理订单异常", e);
					handlePendingList();
				}
			}
		}

		private void handlePendingList() {
			while (true) {
				try {
					List<MapRecord<String, Object, Object>> records = stringRedisTemplate.opsForStream().read(
							Consumer.from(GROUP_NAME, consumerName),
							StreamReadOptions.empty().count(1),
							StreamOffset.create(QUEUE_NAME, ReadOffset.from("0"))
					);
					if (records == null || records.isEmpty()) {
						break;
					}

					MapRecord<String, Object, Object> record = records.get(0);
					Map<Object, Object> value = record.getValue();
					VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
					handleVoucherOrder(voucherOrder);
					stringRedisTemplate.opsForStream().acknowledge(QUEUE_NAME, GROUP_NAME, record.getId());
				} catch (Exception e) {
					log.error("处理pending-list订单异常", e);
					try {
						Thread.sleep(20);
					} catch (InterruptedException ex) {
						Thread.currentThread().interrupt();
						return;
					}
				}
			}
		}
	}

	private void handleVoucherOrder(VoucherOrder voucherOrder) {
		Long userId = voucherOrder.getUserId();
		RLock lock = redissonClient.getLock("lock:order:" + userId);
		boolean isLock = lock.tryLock();
		if (!isLock) {
			log.error("不允许重复下单");
			return;
		}
		try {
			proxy.createVoucherOrder(voucherOrder);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public Result seckillVoucher(Long voucherId) {
		Long userId = UserHolder.getUser().getId();
		long orderId = redisWorker.nextId("order");

		// 1. 执行lua脚本
		Long result = stringRedisTemplate.execute(
				SECKILL_SCRIPT,
				Collections.emptyList(),
				voucherId.toString(),
				userId.toString(),
				String.valueOf(orderId)
		);
		if (result == null) {
			return Result.fail("下单失败，请稍后重试");
		}
		int r = result.intValue();
		// 2. 判断结果是否为0
		if (r != 0) {
			return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
		}

		// 3. 返回订单id，订单创建由stream异步消费者完成
		return Result.ok(orderId);
	}

	@Override
	@Transactional
	public Result createVoucherOrder(Long voucherId) {
		UserDTO user = UserHolder.getUser();
		if (user == null) {
			return Result.fail("请先登录");
		}
		Long userId = user.getId();

		Integer count = query()
				.eq("user_id", userId)
				.eq("voucher_id", voucherId)
				.count();
		if (count > 0) {
			return Result.fail("用户已经购买过一次");
		}

		// 单条 SQL 原子扣减，库存必须 > 0 才能成功
		boolean success = seckillVoucherService.update()
				.setSql("stock = stock - 1")
				.eq("voucher_id", voucherId)
				.gt("stock", 0)
				.update();
		if (!success) {
			return Result.fail("库存不足");
		}

		VoucherOrder voucherOrder = new VoucherOrder();
		long orderId = redisWorker.nextId("order");
		voucherOrder.setId(orderId);
		voucherOrder.setUserId(userId);
		voucherOrder.setVoucherId(voucherId);
		save(voucherOrder);

		return Result.ok(orderId);
	}

	@Override
	@Transactional
	public void createVoucherOrder(VoucherOrder voucherOrder) {
		Long userId = voucherOrder.getUserId();
		Long voucherId = voucherOrder.getVoucherId();

		Integer count = query()
				.eq("user_id", userId)
				.eq("voucher_id", voucherId)
				.count();
		if (count > 0) {
			log.error("用户已经购买过一次");
			return;
		}

		boolean success = seckillVoucherService.update()
				.setSql("stock = stock - 1")
				.eq("voucher_id", voucherId)
				.gt("stock", 0)
				.update();
		if (!success) {
			log.error("库存不足");
			return;
		}

		save(voucherOrder);
	}

}
