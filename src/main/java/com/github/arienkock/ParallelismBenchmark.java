/*
 * Copyright (c) 2014, Oracle America, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 *  * Neither the name of Oracle nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.github.arienkock;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.*;
import java.util.concurrent.ForkJoinPool.ManagedBlocker;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.RandomStringUtils;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import co.paralleluniverse.concurrent.forkjoin.MonitoredForkJoinPool;
import co.paralleluniverse.fibers.*;
import co.paralleluniverse.fibers.io.FiberFileChannel;
import co.paralleluniverse.strands.SuspendableRunnable;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

@SuppressWarnings("serial")
@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Benchmark)
public class ParallelismBenchmark {

	private static final int IO_TOKENS = 1_000;
	private static final int COMPUTE_TOKENS = 25_000_000;
	private Charset charset = Charset.forName("UTF-8");
	private File testFile = new File(Paths.get(System.getProperty("user.dir"))
			.toFile(), "parallelismbenchmarktestdata.txt");
	private ExecutorService cachedThreadPool;
	public static final int BUFFER_SIZE = 8000;
	
	private static AtomicInteger monitor = new AtomicInteger(0);
	private static int correctNumberOfBytes = -1;
	@Param({"0", "256"})
	private int COMPUTE_RUNS;
	@Param({"2", "4", "8"})
	private int IO_RUNS;

	public ParallelismBenchmark() {
		// Create random test data
		if (!testFile.exists() || testFile.length() == 0) {
			try (OutputStreamWriter writer = new OutputStreamWriter(
					new FileOutputStream(testFile), charset)) {
				for (int i = 0; i < 3_000_000; i++) {
					double random = Math.random();
					if (random < 0.05D) {
						writer.append(System.lineSeparator());
					} else if (random < 0.5D) {
						writer.append(" ");
					} else {
						writer.append(RandomStringUtils.randomAlphabetic(1));
					}
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}
	
	
	@Setup
	public void init() {
		monitor.set(0);
		if (cachedThreadPool == null) {
			cachedThreadPool = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setDaemon(true).build());
		}
	}
	
	@TearDown
	public void shutdown() {
		// verify number of bytes read after each invocation
		if (correctNumberOfBytes < 0) {
			correctNumberOfBytes = monitor.get();
		}
		if (monitor.get() != correctNumberOfBytes) {
			throw new AssertionError("Something is wrong with the implementation. Number of bytes read must be constant for each test.");
		}
	}


	/**
	 * FIBERS 
	 */
	SuspendableRunnable ioRunnableF = new SuspendableRunnable() {
		@Override
		public void run() throws SuspendExecution {
			try (FiberFileChannel ch = FiberFileChannel.open((MonitoredForkJoinPool)DefaultFiberScheduler.getInstance().getExecutor(), testFile.toPath(), Collections.EMPTY_SET)) {
				ByteBuffer dst = ByteBuffer.allocateDirect(BUFFER_SIZE);
				for (int read = 0; read >= 0; read = ch.read(dst)) {
					Blackhole.consumeCPU(read * IO_TOKENS);
					monitor.getAndAdd(read);
					dst.hashCode();
					dst.clear();
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	};

	SuspendableRunnable computeRunnableF = new SuspendableRunnable() {
		@Override
		public void run() throws SuspendExecution {
			Blackhole.consumeCPU(COMPUTE_TOKENS);
		}
	};

	@Benchmark
	public void testFiberChannel() throws IOException, Exception {
		ArrayList<Fiber<Void>> list = new ArrayList<Fiber<Void>>();
		for (int i = 0; i < IO_RUNS; i++) {
			list.add(new Fiber<Void>(ioRunnableF).start());
		}
		for (int i = 0; i < COMPUTE_RUNS; i++) {
			list.add(new Fiber<Void>(computeRunnableF).start());
		}
		for (Fiber<Void> f : list) {
			f.join();
		}
	}
	
	
	/**
	 * THREADS ASYNC IO
	 */
	
	Runnable ioRunnableT = new Runnable() {
		@Override
		public void run() {
			try {
				AsynchronousFileChannel ch = AsynchronousFileChannel
						.open(testFile.toPath(), Collections.EMPTY_SET, /*fiberFileThreadPool*/ForkJoinPool.commonPool());
				ByteBuffer dst = ByteBuffer.allocateDirect(BUFFER_SIZE);
				doRead(ch, dst, 0L);
			} catch (Throwable t) {
				throw new RuntimeException(t);
			}
		}

	};
	private void doRead(final AsynchronousFileChannel ch, final ByteBuffer dst, final long position) {
		ch.read(dst, position, null,
				new CompletionHandler<Integer, Void>() {
			
			@Override
			public void completed(Integer result, Void attachment) {
				AtomicInteger m = monitor;
				ByteBuffer d = dst;
				int numRead = result;
				if (numRead >= 0) {
					Blackhole.consumeCPU(numRead * IO_TOKENS);
					m.getAndAdd(numRead);
					d.hashCode();
					d.clear();
					doRead(ch, d, position + numRead);
				} else {
					try {
						ch.close();
					} catch (IOException e) {
					}
				}
			}
			
			@Override
			public void failed(Throwable exc, Void attachment) {
				try {
					ch.close();
				} catch (IOException e) {
				}
				throw new RuntimeException(exc);
			}
		});
	}
	Runnable computeRunnableT = new Runnable() {
		@Override
		public void run() {
			Blackhole.consumeCPU(COMPUTE_TOKENS);
		}
	};

	@Benchmark
	public void testAsyncChannelOnFJP() throws IOException, Exception {
		ArrayList<ForkJoinTask> list = new ArrayList<ForkJoinTask>();
		for (int i = 0; i < IO_RUNS; i++) {
			list.add(ForkJoinTask.adapt(ioRunnableT).fork());
		}
		for (int i = 0; i < COMPUTE_RUNS; i++) {
			list.add(ForkJoinTask.adapt(computeRunnableT).fork());
		}
		ForkJoinPool.commonPool().awaitQuiescence(1, TimeUnit.MINUTES);
//		for (ForkJoinTask f : list) {
//			f.join();
//		}
	}
	
	/**
	 * THREAD MANAGED BLOCKING IO
	 */
	Runnable ioRunnableTB = new Runnable() {
		@Override
		public void run() {
			try (FileChannel ch = FileChannel.open(testFile.toPath(), Collections.EMPTY_SET)) {
				ByteBuffer dst = ByteBuffer.allocateDirect(BUFFER_SIZE);
//				for (int read = 0; read >= 0;read = ch.read(dst)) {
				while (ch.isOpen()) {
					ForkJoinPool.managedBlock(blockingRead(ch, dst));
					int read = dst.position();
					monitor.getAndAdd(read);
					Blackhole.consumeCPU(read * IO_TOKENS);
					dst.hashCode();
					dst.clear();
				}
			} catch (IOException | InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
	};
	protected ManagedBlocker blockingRead(FileChannel ch, ByteBuffer dst) {
		return new ManagedBlocker() {
			boolean done = false;
			private int numRead = 0;
			
			@Override
			public boolean isReleasable() {
				return done;
			}
			
			@Override
			public boolean block() throws InterruptedException {
				try {
					numRead = ch.read(dst);
				} catch (IOException e) {
					throw new RuntimeException(e);
				} finally {
					if (numRead < 0) {
						try {
							ch.close();
						} catch (IOException e) {
						}
					}
				}
				return done = true;
			}
		};
	}

	Runnable computeRunnableTB = new Runnable() {
		@Override
		public void run() {
			Blackhole.consumeCPU(COMPUTE_TOKENS);
		}
	};
	
	@Benchmark
	public void testBlockingOnFJP() throws IOException, Exception {
		ArrayList<ForkJoinTask> list = new ArrayList<ForkJoinTask>();
		for (int i = 0; i < IO_RUNS; i++) {
			list.add(ForkJoinTask.adapt(ioRunnableTB).fork());
		}
		for (int i = 0; i < COMPUTE_RUNS; i++) {
			list.add(ForkJoinTask.adapt(computeRunnableTB).fork());
		}
//		ForkJoinPool.commonPool().awaitQuiescence(1, TimeUnit.MINUTES);
		for (ForkJoinTask f : list) {
			f.join();
		}
	}

	/**
	 * THREAD BLOCKING FULL IO
	 */
	Runnable ioRunnableTBF = new Runnable() {
		@Override
		public void run() {
			try (FileChannel ch = FileChannel.open(testFile.toPath(), Collections.EMPTY_SET)) {
				ByteBuffer dst = ByteBuffer.allocateDirect(BUFFER_SIZE);
				int read = 0;
				while ((read = ch.read(dst)) >= 0) {
					monitor.getAndAdd(read);
					Blackhole.consumeCPU(read * IO_TOKENS);
					dst.hashCode();
					dst.clear();
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	};

	Runnable computeRunnableTBF = new Runnable() {
		@Override
		public void run() {
			Blackhole.consumeCPU(COMPUTE_TOKENS);
		}
	};
	
	@Benchmark
	public void testFullBlockingOnFJP() throws IOException, Exception {
		ArrayList<Future> list = new ArrayList<Future>();
		for (int i = 0; i < IO_RUNS; i++) {
			list.add(cachedThreadPool.submit(ioRunnableTBF));
		}
		for (int i = 0; i < COMPUTE_RUNS; i++) {
			list.add(cachedThreadPool.submit(computeRunnableTBF));
		}
		for (Future f : list) {
			f.get();
		}
	}

}
