package com.group2.eece411;

import java.math.BigInteger;
import java.util.concurrent.ConcurrentHashMap;

public class KVStore {

	private ConcurrentHashMap<BigInteger, byte[]> map = new ConcurrentHashMap<BigInteger, byte[]>();

	public KVStore() {
		map = new ConcurrentHashMap<BigInteger, byte[]>();
	}

	public KVStore(int initialCapacity) {
		map = new ConcurrentHashMap<BigInteger, byte[]>(initialCapacity);
	}

	public byte[] get(BigInteger key) {
		System.out.println("map.len: " + map.size());
		return map.get(key);
	}

	public byte[] put(BigInteger key, byte[] value) {
		return map.put(key, value);
	}

	public byte[] remove(BigInteger key) {
		return map.remove(key);
	}

	public int size() {
		return map.size();
	}
	public ConcurrentHashMap<BigInteger, byte[]> getMap(){
		return map;
	}
}
