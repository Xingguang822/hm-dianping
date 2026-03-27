package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisWorker;
import com.hmdp.utils.UserHolder;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
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

	private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
	static {
		SECKILL_SCRIPT = new DefaultRedisScript<>();
		SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
		SECKILL_SCRIPT.setResultType(Long.class);
	}

	private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
	private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

	@PostConstruct
	private void init() {
		SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
	}

	private class VoucherOrderHandler implements Runnable {
		@Override
		public void run() {
			while (true) {
				try {
					VoucherOrder voucherOrder = orderTasks.take();
					handleVoucherOrder(voucherOrder);
				} catch (Exception e) {
					log.error("处理订单异常", e);
				}
			}
		}
	}

	private IVoucherOrderService proxy;

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

		// 1. 执行lua脚本
		Long result = stringRedisTemplate.execute(
				SECKILL_SCRIPT,
				Collections.emptyList(),
				voucherId.toString(),
				userId.toString()
		);
		int r = result.intValue();
		// 2. 判断结果是否为0
		if (r != 0) {
			return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
		}
		
		// 3. 为0，有购买资格，把下单信息保存到阻塞队列
		long orderId = redisWorker.nextId("order");
		VoucherOrder voucherOrder = new VoucherOrder();
		voucherOrder.setId(orderId);
		voucherOrder.setUserId(userId);
		voucherOrder.setVoucherId(voucherId);

		// 获取代理对象并赋值，以便在异步线程中使用
		proxy = (IVoucherOrderService) AopContext.currentProxy();

		orderTasks.add(voucherOrder);

		// 4. 返回订单id
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
