package util;

import java.util.Map;

public class MapUtil {
	public static <K, V> void printFirstXEntriesFromMap(Map<K, V> map, int x) {
		for(Map.Entry<K, V> entry : map.entrySet()) {
			if(x == 0) {
				break;
			}
			System.out.println(entry);
			--x;
		}
	}
}
