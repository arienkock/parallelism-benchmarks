package com.github.arienkock;

import java.util.concurrent.TimeUnit;

public class GoFiber {

	public static void main(String[] args) throws Exception {
		ParallelismBenchmark parallelismBenchmark = new ParallelismBenchmark();
		parallelismBenchmark.testFiberChannel();
		parallelismBenchmark.testFiberChannel();
		parallelismBenchmark.testFiberChannel();
		long start = System.nanoTime();
		parallelismBenchmark.testFiberChannel();
		parallelismBenchmark.testFiberChannel();
		parallelismBenchmark.testFiberChannel();
		parallelismBenchmark.testFiberChannel();
		parallelismBenchmark.testFiberChannel();
		parallelismBenchmark.testFiberChannel();
		parallelismBenchmark.testFiberChannel();
		parallelismBenchmark.testFiberChannel();
		parallelismBenchmark.testFiberChannel();
		parallelismBenchmark.testFiberChannel();
		parallelismBenchmark.testFiberChannel();
		parallelismBenchmark.testFiberChannel();
		System.out.println(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
	}

}
