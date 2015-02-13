package com.github.arienkock;

import java.util.concurrent.TimeUnit;

public class GoThread {

	public static void main(String[] args) throws Exception {
		ParallelismBenchmark parallelismBenchmark = new ParallelismBenchmark();
		parallelismBenchmark.testAsyncChannelOnFJP();
		parallelismBenchmark.testAsyncChannelOnFJP();
		parallelismBenchmark.testAsyncChannelOnFJP();
		long start = System.nanoTime();
		parallelismBenchmark.testAsyncChannelOnFJP();
		parallelismBenchmark.testAsyncChannelOnFJP();
		parallelismBenchmark.testAsyncChannelOnFJP();
		parallelismBenchmark.testAsyncChannelOnFJP();
		parallelismBenchmark.testAsyncChannelOnFJP();
		parallelismBenchmark.testAsyncChannelOnFJP();
		parallelismBenchmark.testAsyncChannelOnFJP();
		parallelismBenchmark.testAsyncChannelOnFJP();
		parallelismBenchmark.testAsyncChannelOnFJP();
		parallelismBenchmark.testAsyncChannelOnFJP();
		parallelismBenchmark.testAsyncChannelOnFJP();
		parallelismBenchmark.testAsyncChannelOnFJP();
		System.out.println(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
	}

}
