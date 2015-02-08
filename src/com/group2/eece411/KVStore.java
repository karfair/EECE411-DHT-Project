package com.group2.eece411;

import java.util.concurrent.ConcurrentHashMap;

public class KVStore {

	private ConcurrentHashMap<String, byte[]> map = new ConcurrentHashMap<String, byte[]>();

	public KVStore() {
		map = new ConcurrentHashMap<String, byte[]>();
	}

	public KVStore(int initialCapacity) {
		map = new ConcurrentHashMap<String, byte[]>(initialCapacity);
	}

	public byte[] get(String key) {
		System.out.println("map.len: " + map.size());
		return map.get(key);
	}

	public byte[] put(String key, byte[] value) {
		return map.put(key, value);
	}

	public byte[] remove(String key) {
		return map.remove(key);
	}

	public int size() {
		return map.size();
	}
}
