package com.github.arienkock;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.util.*;
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
@Measurement(iterations = 15, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Benchmark)
public class ParallelismBenchmark {

	private static final int IO_TOKENS = 100;
	private static final int COMPUTE_TOKENS = 15_000_000;
	private Charset charset = Charset.forName("UTF-8");
	private Path [] testFilePaths;
	private ExecutorService cachedThreadPool;
	public static final int BUFFER_SIZE = 8000;
	
	private static AtomicInteger monitor = new AtomicInteger(0);
	private static int correctNumberOfBytes = -1;
	@Param({"0", "256"})
	private int COMPUTE_RUNS;
	@Param({"2", "4", "8"})
	private int IO_RUNS;

	
	
	@Setup
	public void init() {
		monitor.set(0);
		if (testFilePaths == null) {
			testFilePaths = new Path [IO_RUNS];
			for (int i=0; i<IO_RUNS; i++) {
				Path path = pathForNum(i);
				if (!Files.exists(path)) {
					try (BufferedWriter writer = Files.newBufferedWriter(path, charset)) {
						for (int j = 0; j < 3_000_000; j++) {
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
		}
	}

	private Path pathForNum(int i) {
		return Paths.get(System.getProperty("user.dir")).resolve("parallelismbenchmarktestdata"+i+".txt");
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
		testFilePaths = null;
		Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
		StringBuilder sb = new StringBuilder(System.lineSeparator()).append("Thread dump:").append(System.lineSeparator());
		for (Iterator<Thread> iterator = threadSet.iterator(); iterator
				.hasNext();) {
			Thread thread = iterator.next();
			sb.append(thread.getName()).append(System.lineSeparator());
		}
		System.out.println(sb);
	}


	/**
	 * FIBERS 
	 */
	public SuspendableRunnable ioRunnableF(Path path) {
		return new SuspendableRunnable() {
			@Override
			public void run() throws SuspendExecution {
				try (FiberFileChannel ch = FiberFileChannel.open((MonitoredForkJoinPool)DefaultFiberScheduler.getInstance().getExecutor(), path, Collections.emptySet())) {
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
	}

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
			list.add(new Fiber<Void>(ioRunnableF(pathForNum(i))).start());
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
	
	public Runnable ioRunnableT(Path path) {
		return new Runnable() {
			@Override
			public void run() {
				try {
					AsynchronousFileChannel ch = AsynchronousFileChannel.open(path, Collections.emptySet(), /*fiberFileThreadPool*/ForkJoinPool.commonPool());
					ByteBuffer dst = ByteBuffer.allocateDirect(BUFFER_SIZE);
					doRead(ch, dst, 0L);
				} catch (Throwable t) {
					throw new RuntimeException(t);
				}
			}
	
		};
	}
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
		ArrayList<ForkJoinTask<?>> list = new ArrayList<ForkJoinTask<?>>();
		for (int i = 0; i < IO_RUNS; i++) {
			list.add(ForkJoinTask.adapt(ioRunnableT(pathForNum(i))).fork());
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
	public Runnable ioRunnableTB(Path path) {
		return new Runnable() {
			@Override
			public void run() {
				try (FileChannel ch = FileChannel.open(path, Collections.emptySet())) {
					ByteBuffer dst = ByteBuffer.allocateDirect(BUFFER_SIZE);
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
	}
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

	@Benchmark
	public void testBlockingOnFJP() throws IOException, Exception {
		ArrayList<ForkJoinTask<?>> list = new ArrayList<ForkJoinTask<?>>();
		for (int i = 0; i < IO_RUNS; i++) {
			list.add(ForkJoinTask.adapt(ioRunnableTB(pathForNum(i))).fork());
		}
		for (int i = 0; i < COMPUTE_RUNS; i++) {
			list.add(ForkJoinTask.adapt(computeRunnableT).fork());
		}
		for (ForkJoinTask<?> f : list) {
			f.join();
		}
	}

	/**
	 * THREAD BLOCKING FULL IO
	 */
	public Runnable ioRunnableTBF(Path path) { 
		return new Runnable() {
			@Override
			public void run() {
				try (FileChannel ch = FileChannel.open(path, Collections.emptySet())) {
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
	}

	@Benchmark
	public void testFullBlockingOnFJP() throws IOException, Exception {
		if (cachedThreadPool == null) {
			cachedThreadPool = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setDaemon(true).build());
		}
		ArrayList<Future<?>> list = new ArrayList<Future<?>>();
		for (int i = 0; i < IO_RUNS; i++) {
			list.add(cachedThreadPool.submit(ioRunnableTBF(pathForNum(i))));
		}
		for (int i = 0; i < COMPUTE_RUNS; i++) {
			list.add(cachedThreadPool.submit(computeRunnableT));
		}
		for (Future<?> f : list) {
			f.get();
		}
	}

}
