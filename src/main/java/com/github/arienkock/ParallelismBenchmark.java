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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.commons.lang3.RandomStringUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import co.paralleluniverse.fibers.Fiber;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.fibers.io.FiberFileChannel;
import co.paralleluniverse.strands.SuspendableRunnable;

@SuppressWarnings("serial")
@State(Scope.Benchmark)
public class ParallelismBenchmark {

	private Charset charset = Charset.forName("UTF-8");

	static HashSet<String> commonWords = new HashSet<>(Arrays.asList("the",
			"be", "to", "of", "and", "a", "in", "that", "have", "I", "it",
			"for", "not", "on", "with"));
	private File testFile = new File(Paths.get(System.getProperty("user.dir"))
			.toFile(), "parallelismbenchmarktestdata.txt");
	private Pattern whitespace = Pattern.compile("[\\s]+");
	private AtomicInteger cachedResult = new AtomicInteger(0);
	public static final int BUFFER_SIZE = 200;

	public ParallelismBenchmark() {
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

	@TearDown
	public void tearDown() {
		System.out.println("actual reslt = " + cachedResult.get());
	}

	// @Benchmark
	public void testMethod() {
	}

	// @Benchmark
	public void readFile() throws IOException {
		Files.readAllLines(testFile.toPath(), charset);
	}
//
//	@Benchmark
//	public void testFiberFiberChannel() throws IOException, Exception {
//		new Fiber<Void>(new SuspendableRunnable() {
//			@Override
//			public void run() throws SuspendExecution {
//				try (StringSourceI ls = 
//						new ByteChannelLineSource(FiberFileChannel.open(testFile.toPath()), charset, BUFFER_SIZE)
//				) {
//					CharSequence seq = null;
//					while ((seq = ls.readString()) != null) {
////						System.out.print(seq);
//					}
//				} catch (IOException e) {
//					throw new RuntimeException(e);
//				}
//			}
//		}).start().join();
//	}
////	@Benchmark
//	public void testThreadFileChannel() throws IOException, Exception {
//		Thread thread = new Thread(new Runnable() {
//			@Override
//			public void run()  {
//				try (StringSourceI ls = 
//						new ByteChannelLineSource(FileChannel.open(testFile.toPath()), charset, BUFFER_SIZE)
//				) {
//					CharSequence seq = null;
//					while ((seq = ls.readString()) != null) {
////						System.out.print(seq);
//					}
//				} catch (Throwable e) {
//					throw new RuntimeException(e);
//				}
//			}
//		});
//		thread.start();
//		thread.join();
//	}
//	@Benchmark
//	public void testFiberBufferedReader() throws IOException, Exception {
//		testFiberAsyncFile(()->{try {
//			return new LineSourceWrapper(new BufferedReader(new InputStreamReader(new FileInputStream(testFile), charset), BUFFER_SIZE));
//		} catch (Exception e) {
//			throw new RuntimeException(e);
//		}});
//	}
//	
//	@Benchmark
//	public void testFiberFileChannel() throws IOException, Exception {
//		testFiberAsyncFile(()->{try {
//			return new ByteChannelLineSource(FileChannel.open(testFile.toPath()), charset, BUFFER_SIZE);
//		} catch (Exception e) {
//			throw new RuntimeException(e);
//		}});
//	}
//	
//	public void testFiberAsyncFile(Supplier<StringSourceI> sourceSource) throws Exception {
//		new Fiber<Void>(new SuspendableRunnable() {
//			@Override
//			public void run() throws SuspendExecution {
//				try (StringSourceI ls = 
//						sourceSource.get()
//				) {
//					CharSequence seq = null;
//					while ((seq = ls.readString()) != null) {
////						System.out.print(seq);
//					}
//				} catch (IOException e) {
//					throw new RuntimeException(e);
//				}
//			}
//		}).start().join();
//	}

	@Benchmark
	public void quasar100() throws InterruptedException, ExecutionException,
			IOException {
		quasar(100);
	}

	@Benchmark
	public void quasar1250() throws InterruptedException, ExecutionException,
			IOException {
		quasar(1250);
	}

	public void quasar(int batchSize) throws InterruptedException,
			ExecutionException, IOException {
		new Fiber<Void>(new SuspendableRunnable() {
			@Override
			public void run() throws SuspendExecution, InterruptedException {
				try (StringSourceI reader = new ByteChannelLineSource(FiberFileChannel.open(testFile.toPath()),
						charset, BUFFER_SIZE)) {
					Integer count = new CountWordsFiber(reader, batchSize).start()
							.get();
					// Integer count = new CountWordsFiber(testFile.toPath(), charset,
					// batchSize).start().get();
					cachedResult.compareAndSet(0, (int) count);
					if (cachedResult.get() != count) {
						throw new AssertionError();
					}
				} catch (ExecutionException | IOException e) {
					e.printStackTrace();
				}
			}
		}).start().join();
	}

	// @Benchmark
	public void streamProcessingParallel() throws IOException {
		streamProcessing(true);
	}

	// @Benchmark
	public void streamProcessingNonParallel() throws IOException {
		streamProcessing(false);
	}

	 @Benchmark
	public void forkJoin500() throws InterruptedException, ExecutionException {
		forkJoin(500);
	}

	 @Benchmark
	public void forkJoin750() throws InterruptedException, ExecutionException {
		forkJoin(750);
	}

	@Benchmark
	public void forkJoin1250() throws InterruptedException, ExecutionException {
		forkJoin(1250);
	}

	public void forkJoin(int batchSize) throws InterruptedException,
			ExecutionException {
		try (StringSourceI reader = new ByteChannelLineSource(FileChannel.open(testFile.toPath()),
				charset, BUFFER_SIZE)) {
			Integer count = new CountWordsForkTask(reader, batchSize).fork()
					.join();
			cachedResult.compareAndSet(0, (int) count);
			if (cachedResult.get() != count) {
				throw new AssertionError();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void streamProcessing(boolean parallel) throws IOException {
		try (Stream<String> stream = Files.lines(testFile.toPath(), charset)) {
			Stream<String> streamToUse = stream;
			if (parallel) {
				streamToUse = stream.parallel();
			}
			long count = streamToUse.flatMap(whitespace::splitAsStream)
					.map(String::toLowerCase).filter(commonWords::contains)
					.count();
			cachedResult.compareAndSet(0, (int) count);
			if (cachedResult.get() != count) {
				throw new AssertionError();
			}
		}
	}

}
