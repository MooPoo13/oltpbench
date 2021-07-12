package de.ukr.benchmarks.cdabench;

import static de.ukr.benchmarks.cdabench.CDAConfig.*; 
import java.util.Random;

public class CDAUtil {
	public static String getCurrentTime() {
		return dateFormat.format(new java.util.Date());
	}
	
	public static int randomNumber(int min, int max, Random r) {
		return (int) (r.nextDouble() * (max - min + 1) + min);
	}
}
