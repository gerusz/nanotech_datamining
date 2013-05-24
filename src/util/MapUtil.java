package util;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class MapUtil {
	public static abstract class DoForEachElement<K, V, X, Y> {
		protected Map<K, V> map;
		protected Map<X, Y> outcome;

		public DoForEachElement(Map<K,V> map, Map<X, Y> outcome) {
			this.map = map;
			this.outcome = outcome;
		}
		
		public void doTheJob() {
			for(Map.Entry<K, V> entry : map.entrySet()) {
				doForEachElement(entry.getKey(), entry.getValue());
			}
		}
		
		protected abstract void doForEachElement(K key, V val);
		
		public Map<X, Y> getOutcome() {
			return outcome;
		}
	}
	public static <K, V> void printFirstXEntriesFromMap(Map<K, V> map, int x) {
		for(Map.Entry<K, V> entry : map.entrySet()) {
			if(x == 0) {
				break;
			}
			System.out.println(entry);
			--x;
		}
	}
	
	public static <K> void removeEntries(Map<K, Integer> map, Set<K> toRemove) {
		for(K k : toRemove) {
			map.remove(k);
		}
	}
	
	public static <K> void plusAtIndex(Map<K, Integer> map, K key, int value) {
		Integer i = 0;
		if(map.containsKey(key)) {
			i = map.get(key);
		}
		i += value;
		map.put(key, i);
	}
	
	public static <K> void mapAddition(Map<K, Integer> destMap, Map<K, Integer> addMap) {
		for(Map.Entry<K, Integer> entry : addMap.entrySet()) {
			plusAtIndex(destMap, entry.getKey(), entry.getValue());
		}
	}
	
	public static <K> void mapIncrement(Map<K, AtomicInteger> map, Collection<K> forEntries) {
		for(K key : forEntries) {
			map.get(key).incrementAndGet();
		}
	}
	
	public static <K> void addHighestFromMapToNewMap(Map<K, Integer> map, Map<K, Integer> newMap) {
		DoForEachElement<K, Integer, K, Integer> doit = new DoForEachElement<K, Integer, K, Integer>(map, newMap) {

			@Override
			protected void doForEachElement(K key, Integer val) {
				Integer max = val;
				if(outcome.containsKey(key)) {
					Integer storedVal = outcome.get(key);
					max = storedVal > val ? storedVal : val;
				}
				outcome.put(key, max);
			}
		};
		doit.doTheJob();
	}

	public static <K, V, Y> void addToMapFromMapWithDefaultVal(
			Map<K, Y> outcome,
			Map<K, V> input, final Y defaultVal) {
		DoForEachElement<K, V, K, Y> doit = new DoForEachElement<K, V, K, Y>(input, outcome) {

			@Override
			protected void doForEachElement(K key, V val) {
				if(!outcome.containsKey(key)) {
					outcome.put(key, defaultVal);
				}
			}
		};
		
		doit.doTheJob();
	}

	public static <K> Set<K> getEntriesSmallerThan(
			Map<K, Integer> map, int value) {
		Set<K> toRemove = new HashSet<>();
		for(Map.Entry<K, Integer> e : map.entrySet()) {
			if(e.getValue() < value) {
				toRemove.add(e.getKey());
			}
		}
		return toRemove;
	}
}
