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
import org.springframework.aop.framework.AopContext;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

	@Resource
	private ISeckillVoucherService seckillVoucherService;

	@Resource
	private RedisWorker redisWorker;

	@Override
	public Result seckillVoucher(Long voucherId) {
		SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
		if (voucher == null) {
			return Result.fail("优惠券不存在");
		}
		LocalDateTime now = LocalDateTime.now();
		if (now.isBefore(voucher.getBeginTime())) {
			return Result.fail("秒杀尚未开始");
		}
		if (now.isAfter(voucher.getEndTime())) {
			return Result.fail("秒杀已结束");
		}
		if (voucher.getStock() < 1) {
			return Result.fail("库存不足");
		}

		UserDTO user = UserHolder.getUser();
		if (user == null) {
			return Result.fail("请先登录");
		}
		Long userId = user.getId();

		// 悲观锁：同一用户串行下单，避免并发通过一人一单校验
		synchronized (userId.toString().intern()) {
			IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
			return proxy.createVoucherOrder(voucherId);
		}
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

}
