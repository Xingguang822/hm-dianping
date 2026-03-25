package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TYPE_KEY;
import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TYPE_TTL;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

	@Resource
	private StringRedisTemplate stringRedisTemplate;

	@Override
	public Result queryTypeList() {
		String typeListJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_TYPE_KEY);
		if (StrUtil.isNotBlank(typeListJson)) {
			List<ShopType> typeList = JSONUtil.toList(typeListJson, ShopType.class);
			return Result.ok(typeList);
		}

		List<ShopType> typeList = query().orderByAsc("sort").list();
		if (typeList == null || typeList.isEmpty()) {
			return Result.fail("店铺分类不存在！");
		}

		stringRedisTemplate.opsForValue().set(
				CACHE_SHOP_TYPE_KEY,
				JSONUtil.toJsonStr(typeList),
				CACHE_SHOP_TYPE_TTL,
				TimeUnit.MINUTES
		);
		return Result.ok(typeList);
	}

}
