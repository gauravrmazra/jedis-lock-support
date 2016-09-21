package com.creativeneons.jedis.lock;

import java.util.concurrent.TimeUnit;

/**
 * Contract to support Distributed Locking
 * @author Mazra, Gaurav Rai
 *
 */
public interface JedisLockSupport {
	
	/**
	 * Acquire the lock on passed key and set its owner
	 * It will try once and will return immediately
	 * @param key the key to set lock against
	 * @param owner lock owner
	 * @return true if lock is acquired
	 */
	public boolean acquire(String key, String owner);
	
	
	/**
	 * Acquire the lock on passed key
	 * It will try to get lock for timeout passed
	 * @param key
	 * @param owner
	 * @param timeout
	 * @param timeUnit
	 * @return true if lock is acquired
	 */
	public boolean tryAcquire(String key, String owner, long timeout, TimeUnit timeUnit);
	
	/**
	 * Releases the lock
	 * @param key
	 * @param owner
	 */
	public void release(String key, String owner);
	
	/**
	 * Interning the string key. Used internally.
	 * String str1 = new String("abc");
	 * String str2 = new String("abc");
	 * str1 and str2 are two different objects 
	 * and we synchronized based on the passed key
	 * 
	 * @param key to normalize
	 * @return the normalized key
	 */
	default String normalizeKey(String key) {
		return key.intern();
	}
}
