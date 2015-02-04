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
import java.nio.channels.Channels;
import java.nio.charset.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.commons.lang3.RandomStringUtils;
import org.openjdk.jmh.annotations.*;

import co.paralleluniverse.fibers.Fiber;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.fibers.io.FiberFileChannel;
import co.paralleluniverse.strands.SuspendableRunnable;

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

	@Benchmark
	@SuppressWarnings("serial")
	public void testFiberAsyncFile() throws Exception {
		CharsetEncoder encoder = charset.newEncoder();
		CharsetDecoder decoder = charset.newDecoder();
		new Fiber<Void>(new SuspendableRunnable() {
			@Override
			public void run() throws SuspendExecution {
				try (LineSourceI ls = new LineSourceWrapper(new BufferedReader(
						Channels.newReader(
								FiberFileChannel.open(testFile.toPath()),
								charset.name())))) {
					CharSequence seq = null;
					while ((seq = ls.readLine()) != null) {
						System.out.println(seq);
					}
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		}).start().join();
	}

	@Benchmark
	public void quasar1000() throws InterruptedException, ExecutionException,
			IOException {
		quasar(1000);
	}

	public void quasar(int batchSize) throws InterruptedException,
			ExecutionException, IOException {
		try (LineSourceI reader = new LineSourceWrapper(new BufferedReader(
				Channels.newReader(FiberFileChannel.open(testFile.toPath()),
						charset.name())))) {
			Integer count = new CountWordsFiber(reader, batchSize).start()
					.get();
			// Integer count = new CountWordsFiber(testFile.toPath(), charset,
			// batchSize).start().get();
			cachedResult.compareAndSet(0, (int) count);
			if (cachedResult.get() != count) {
				throw new AssertionError();
			}
		}
	}

	// @Benchmark
	public void streamProcessingParallel() throws IOException {
		streamProcessing(true);
	}

	// @Benchmark
	public void streamProcessingNonParallel() throws IOException {
		streamProcessing(false);
	}

	// @Benchmark
	public void forkJoin1000() throws InterruptedException, ExecutionException {
		forkJoin(1000);
	}

	// @Benchmark
	public void forkJoin750() throws InterruptedException, ExecutionException {
		forkJoin(750);
	}

	// @Benchmark
	public void forkJoin1250() throws InterruptedException, ExecutionException {
		forkJoin(1250);
	}

	public void forkJoin(int batchSize) throws InterruptedException,
			ExecutionException {
		try (LineSourceWrapper reader = new LineSourceWrapper(
				new BufferedReader(new InputStreamReader(new FileInputStream(
						testFile), charset)))) {
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
