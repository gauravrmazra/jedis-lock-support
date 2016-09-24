package com.creativeneons.jedis.lock;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * Single cluster lock support using Jedis
 * 
 * @author Mazra, Gaurav Rai
 *
 */
public class SingleClusterJedisLockSupport implements JedisLockSupport {

	private static final long MIN_WAIT_MS = 100;
	private JedisPool jedisPool;
	private long lockAcquireTime;

	public SingleClusterJedisLockSupport(JedisPool jedisPool, long lockAcquireTime) {
		super();
		this.jedisPool = jedisPool;
		this.lockAcquireTime = lockAcquireTime;
	}

	@Override
	public boolean acquire(String key, String owner) {
		key = normalizeKey(key);
		synchronized (key) {
			try (Jedis jedis = jedisPool.getResource();) {
				return isMine(key, owner, jedis) || added(key, owner, jedis);
			}
		}
	}

	@Override
	public boolean tryAcquire(String key, String owner, long timeout, TimeUnit timeUnit) {
		boolean acquired = false;
		key = normalizeKey(key);
		long timeToWaitInMs = timeUnit == TimeUnit.MILLISECONDS ? timeout : timeUnit.toMillis(timeout);
		try (Jedis jedis = jedisPool.getResource();) {
			while (!acquired && timeToWaitInMs >= 0) {
				timeToWaitInMs = timeToWaitInMs - MIN_WAIT_MS;
				synchronized (key) {
					acquired = isMine(key, owner, jedis) || added(key, owner, jedis);
				}
				try {
					TimeUnit.MILLISECONDS.wait(MIN_WAIT_MS);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
		}
		return acquired;
	}

	String lua_script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";

	@Override
	public void release(String key, String owner) {
		key = normalizeKey(key);
		synchronized (key) {
			try (Jedis jedis = jedisPool.getResource();) {
				Object result = jedis.eval(lua_script, 1, key, value(key, owner));
				//TODO need to log it
			}
		}
	}

	private boolean isMine(String key, String owner, Jedis jedis) {
		String value = jedis.get(key);
		return Objects.nonNull(value) && value.endsWith(owner);
	}

	private String value(String key, String owner) {
		return new StringBuilder(key.length() + owner.length() + 2).append(key).append("::").append(owner).toString();
	}

	private boolean added(String key, String owner, Jedis jedis) {
		return Objects.nonNull(setIfNotExists(key, owner, jedis));
	}

	private String setIfNotExists(String key, String owner, Jedis jedis) {
		return jedis.set(key, value(key, owner), "nx", "ex", lockAcquireTime);
	}
}
