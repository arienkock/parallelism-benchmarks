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
import java.nio.CharBuffer;
import java.nio.channels.Channels;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.commons.lang3.RandomStringUtils;
import org.openjdk.jmh.annotations.*;

import co.paralleluniverse.fibers.Fiber;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.fibers.io.FiberFileChannel;

@State(Scope.Benchmark)
public class ParallelismBenchmark {

	private Charset charset = Charset.forName("UTF-8");
	
	private static HashSet<String> commonWords = 
			new HashSet<>(Arrays.asList
					("the", "be", "to", "of", "and", "a", "in", "that", "have", "I", "it", "for", "not", "on", "with")
				);
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
		System.out.println("actual reslt = "+cachedResult.get());
	}

//	@Benchmark
	public void testMethod() {
	}

//	@Benchmark
	public void readFile() throws IOException {
		Files.readAllLines(testFile.toPath(), charset);
	}
	
	@Benchmark
	public void quasar1000() throws InterruptedException, ExecutionException, IOException {
		quasar(1000);
	}
	
	public void quasar(int batchSize) throws InterruptedException, ExecutionException, IOException {
		try (BufferedReader reader = new BufferedReader(Channels.newReader(FiberFileChannel.open(testFile.toPath()), charset.name()))) {
			Integer count = new CountWordsFiber(reader, batchSize).start().get();
			cachedResult.compareAndSet(0, (int) count);
			if (cachedResult.get() != count) {
				throw new AssertionError();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

//	@Benchmark
	public void streamProcessingParallel() throws IOException {
		streamProcessing(true);
	}
	
//	@Benchmark
	public void streamProcessingNonParallel() throws IOException {
		streamProcessing(false);
	}
	
//	@Benchmark
	public void forkJoin1000() throws InterruptedException, ExecutionException {
		forkJoin(1000);
	}
//	@Benchmark
	public void forkJoin750() throws InterruptedException, ExecutionException {
		forkJoin(750);
	}
//	@Benchmark
	public void forkJoin1250() throws InterruptedException, ExecutionException {
		forkJoin(1250);
	}
	
	public void forkJoin(int batchSize) throws InterruptedException, ExecutionException {
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(testFile), charset))) {
			Integer count = new CountWordsForkTask(reader, batchSize).fork().join();
			cachedResult.compareAndSet(0, (int) count);
			if (cachedResult.get() != count) {
				throw new AssertionError();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	public static class LineGetter {
		private FiberFileChannel ch;
		private Charset cs;
		public LineGetter(FiberFileChannel ch, Charset cs) {
			this.cs = cs;
			this.ch = ch;
		}
		public String readLine() throws SuspendExecution, InterruptedException{
//			cs.newDecoder().decode(in, out, endOfInput)
			
			CharBuffer cb;
			return null;
		}
	}
	
	@SuppressWarnings("serial")
	public static class CountWordsFiber extends Fiber<Integer>{
		CountWordsTask task;
		public CountWordsFiber(BufferedReader reader, int batchSize) {
			task = new CountWordsTask(reader, batchSize) {
				Fiber<Integer> nextTask;
				@Override
				protected void startNextTast(BufferedReader reader, int batchSize) {
					nextTask = new CountWordsFiber(reader, batchSize).start();
				}
				
				@Override
				protected int getNextTaskResult() throws ExecutionException, InterruptedException {
					return nextTask.get();
				}
			};
		}

		@Override
		public Integer run() throws SuspendExecution, InterruptedException {
			return task.work();
		}
	}
	
	@SuppressWarnings("serial")
	public static class CountWordsForkTask extends RecursiveTask<Integer>{
		CountWordsTask task;
		public CountWordsForkTask(BufferedReader reader, int batchSize) {
			task = new CountWordsTask(reader, batchSize) {
				ForkJoinTask<Integer> nextTask;
				@Override
				protected void startNextTast(BufferedReader reader, int batchSize) {
					nextTask = new CountWordsForkTask(reader, batchSize).fork();
				}
				
				@Override
				protected int getNextTaskResult() {
					return nextTask.join();
				}
			};
		}

		@Override
		protected Integer compute() {
			try {
				return task.work();
			} catch (SuspendExecution e) {
				throw new AssertionError();
			}
		}
	}
	
	public static abstract class CountWordsTask {
		private static Pattern whitespace = Pattern.compile("[\\s]+");
		private BufferedReader reader;
		private int batchSize;
		
		public CountWordsTask(BufferedReader reader, int batchSize) {
			this.reader = reader;
			this.batchSize = batchSize;
		}
		
		protected Integer work() throws SuspendExecution {
			try {
				ArrayDeque<String> deque = new ArrayDeque<>();
				String line = null;
				while ((line = reader.readLine())!=null && deque.size() < batchSize) {
					deque.add(line);
				}
				if (deque.size() == 0) {
					return 0;
				}
				startNextTast(reader, batchSize);
				int count = 0;
				for (String currentLine : deque) {
					String[] split = whitespace.split(currentLine);
					for (int i = 0; i < split.length; i++) {
						if (ParallelismBenchmark.commonWords.contains(split[i].toLowerCase())) {
							count++;
						}
					}
				}
				try {
					return getNextTaskResult() + count;
				} catch (ExecutionException | InterruptedException e) {
					throw new RuntimeException(e);
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		
		protected abstract int getNextTaskResult() throws ExecutionException, InterruptedException; //nextTask.join()
		protected abstract void startNextTast(BufferedReader reader, int batchSize); //	ForkJoinTask<Integer> nextTask = new CountWorksTask(reader, batchSize).fork();
	}
	
	public void streamProcessing(boolean parallel) throws IOException {
		try (Stream<String> stream = Files.lines(testFile.toPath(), charset)) {
			Stream<String> streamToUse = stream;
			if (parallel) {
				streamToUse = stream.parallel();
			}
			long count = streamToUse
				.flatMap(whitespace::splitAsStream)
				.map(String::toLowerCase)
				.filter(commonWords::contains)
				.count();
			cachedResult.compareAndSet(0, (int) count);
			if (cachedResult.get() != count) {
				throw new AssertionError();
			}
		}
	}
	
	
}
