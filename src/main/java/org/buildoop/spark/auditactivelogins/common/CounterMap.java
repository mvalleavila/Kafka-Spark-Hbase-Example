package org.buildoop.spark.auditactivelogins.common;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;

public class CounterMap {
	HashMap<String, Counter> map = new HashMap<String, Counter>();
	
	public void increment(String key, long increment) {
		Counter count = map.get(key);
		if (count == null) {
			count = new Counter();
			map.put(key, count);
		} 
		count.value += increment;
	}
	
	
	public long getValue(String key) {
		Counter count = map.get(key);
		if (count != null) {
			return count.value;
		} else {
			return 0;
		}
	}
	
	public Set<Entry<String, Counter>> entrySet() {
		return map.entrySet();
	}
	
	public void clear() {
		map.clear();
	}
	
	public static class Counter {
		public long value;
	}
	
	
}
