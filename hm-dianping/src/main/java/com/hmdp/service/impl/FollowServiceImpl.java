package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.FOLLOW_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {

	@Resource
	private StringRedisTemplate stringRedisTemplate;

	@Resource
	private IUserService userService;

	@Override
	@Transactional
	public Result follow(Long followUserId, Boolean isFollow) {
		UserDTO user = UserHolder.getUser();
		if (user == null) {
			return Result.fail("请先登录");
		}
		Long userId = user.getId();
		String key = FOLLOW_KEY + userId;
		if (Boolean.TRUE.equals(isFollow)) {
			Integer count = query().eq("user_id", userId)
					.eq("follow_user_id", followUserId)
					.count();
			if (count > 0) {
				return Result.ok();
			}
			Follow follow = new Follow();
			follow.setUserId(userId);
			follow.setFollowUserId(followUserId);
			boolean success = save(follow);
			if (success) {
				stringRedisTemplate.opsForSet().add(key, followUserId.toString());
			}
		} else {
			boolean success = lambdaUpdate()
					.eq(Follow::getUserId, userId)
					.eq(Follow::getFollowUserId, followUserId)
					.remove();
			if (success) {
				stringRedisTemplate.opsForSet().remove(key, followUserId.toString());
			}
		}
		return Result.ok();
	}

	@Override
	public Result isFollow(Long followUserId) {
		UserDTO user = UserHolder.getUser();
		if (user == null) {
			return Result.ok(false);
		}
		Long userId = user.getId();
		Integer count = query().eq("user_id", userId)
				.eq("follow_user_id", followUserId)
				.count();
		return Result.ok(count > 0);
	}

	@Override
	public Result followCommons(Long id) {
		UserDTO user = UserHolder.getUser();
		if (user == null) {
			return Result.ok(Collections.emptyList());
		}
		Long userId = user.getId();
		String key1 = FOLLOW_KEY + userId;
		String key2 = FOLLOW_KEY + id;
		Set<String> intersect = stringRedisTemplate.opsForSet().intersect(key1, key2);
		if (intersect == null || intersect.isEmpty()) {
			return Result.ok(Collections.emptyList());
		}

		List<Long> ids = intersect.stream().map(Long::valueOf).collect(Collectors.toList());
		String idStr = StrUtil.join(",", ids);
		List<UserDTO> users = userService.query()
				.in("id", ids)
				.last("order by field(id," + idStr + ")")
				.list()
				.stream()
				.map(u -> BeanUtil.copyProperties(u, UserDTO.class))
				.collect(Collectors.toList());
		return Result.ok(users);
	}

}
