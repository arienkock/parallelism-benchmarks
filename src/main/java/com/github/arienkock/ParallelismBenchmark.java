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
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;
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
import co.paralleluniverse.fibers.Suspendable;
import co.paralleluniverse.fibers.io.FiberFileChannel;
import co.paralleluniverse.strands.SuspendableRunnable;

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
    public void testFiberAsyncFile() throws Exception {
//		System.out.println("new run");
		CharsetEncoder encoder = charset.newEncoder();
		CharsetDecoder decoder = charset.newDecoder();
        new Fiber(new SuspendableRunnable() {
            @Override
            public void run() throws SuspendExecution {
//                try (FiberFileChannel ch = FiberFileChannel.open(testFile.toPath(), READ, WRITE, CREATE, TRUNCATE_EXISTING)) {
              try (LineSource ls = new LineSource(testFile.toPath(), charset)) {
//              try (LineSourceI ls = new LineSourceWrapper(new BufferedReader(new InputStreamReader(new FileInputStream(testFile), charset)))) {
                    ByteBuffer buf = ByteBuffer.allocateDirect(1024);
                    CharSequence seq=null;
                    while ((seq = ls.readLine()) != null) {
                    	System.out.println("go! " + seq);
                    }

                    
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

            }
        }).start().join();
    }
	
//	@Benchmark
	public void quasar1000() throws InterruptedException, ExecutionException, IOException {
		quasar(1000);
	}
	
	public void quasar(int batchSize) throws InterruptedException, ExecutionException, IOException {
		try (LineSource reader = new LineSource(testFile.toPath(), charset)) {
//			Integer count = new CountWordsFiber(reader, batchSize).start().get();
			Integer count = new CountWordsFiber(testFile.toPath(), charset, batchSize).start().get();
			cachedResult.compareAndSet(0, (int) count);
			if (cachedResult.get() != count) {
				throw new AssertionError();
			}
		}
	}
	public static interface LineSourceI extends AutoCloseable {
		public CharSequence readLine() throws IOException;
		public void close();
	}
	
	public static class LineSourceWrapper implements LineSourceI {
		private BufferedReader reader;

		public LineSourceWrapper(BufferedReader reader) {
			this.reader = reader;
		}
		@Override
		public CharSequence readLine() throws IOException {
			return reader.readLine();
		}

		@Override
		public void close() {
			try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static class LineSource implements LineSourceI {
		private FiberFileChannel channel;
//		private InputStream inputStream;
		private CharsetDecoder decoder;
		private StringBuilder sb = new StringBuilder(8192);
		private ByteBuffer buffer;
		private CharBuffer charBuffer;
		private Path path;
		private boolean done = false;
		private boolean skipLF = false, ignoreLF = false;
//	    private char cb[];
	    private int nChars, nextChar;
	    private static int defaultExpectedLineLength = 80;

		public LineSource(/*FiberFileChannel channel, */Path path, Charset charset) {
			this.path = path;
//			this.inputStream = Channels.newInputStream(channel);
			this.decoder = charset.newDecoder();
			this.buffer = ByteBuffer.allocateDirect(8192);
			this.charBuffer = CharBuffer.allocate(8192);
		}
		
		@Suspendable
		public CharSequence readLine() throws IOException {
	        StringBuffer s = null;
	        int startChar;

	            boolean omitLF = ignoreLF || skipLF;

	        bufferLoop:
	            for (;;) {

	                if (nextChar >= nChars)
	                    fill();
	                if (nextChar >= nChars) { /* EOF */
	                    if (s != null && s.length() > 0)
	                        return s.toString();
	                    else
	                        return null;
	                }
	                boolean eol = false;
	                char c = 0;
	                int i;

	                /* Skip a leftover '\n', if necessary */
	                if (omitLF && (charBuffer.get(nextChar) == '\n'))
	                    nextChar++;
	                skipLF = false;
	                omitLF = false;

	            charLoop:
	                for (i = nextChar; i < nChars; i++) {
	                    c = charBuffer.get(i);
	                    if ((c == '\n') || (c == '\r')) {
	                        eol = true;
	                        break charLoop;
	                    }
	                }

	                startChar = nextChar;
	                nextChar = i;

	                if (eol) {
	                    String str;
	                    if (s == null) {
	                        str = charBuffer.toString();//new String(charBuffer. cb, startChar, i - startChar);
	                    } else {
	                        s.append(charBuffer, startChar, i - startChar);
	                        str = s.toString();
	                    }
	                    nextChar++;
	                    if (c == '\r') {
	                        skipLF = true;
	                    }
	                    return str;
	                }

	                if (s == null)
	                    s = new StringBuffer(defaultExpectedLineLength);
	                s.append(charBuffer, startChar, i - startChar);
	            }
	    }
		
		@Suspendable
		private void fill() throws IOException {
			charBuffer.clear();
			getChannel().read(buffer);			
			buffer.flip();
			decoder.decode(buffer, charBuffer, false);
			charBuffer.flip();
		}

		@Suspendable
		public CharSequence readLine2() throws IOException {
			if (done) {
				return null;
			}
			int nlIndex = -1, numRead = 0;
			while ((nlIndex = sb.indexOf("\n")) < 0 && (numRead = getChannel().read(buffer)) > 0) {
//				System.out.println("nl index = " + nlIndex + "  numRead "+ numRead + " sb.length " + sb.length());
				buffer.flip();
				decoder.decode(buffer, charBuffer, false);
				charBuffer.flip();
				sb.append(charBuffer);
//				System.out.print("Champ!"+sb);
				charBuffer.clear();
				buffer.clear();
			}
			if (nlIndex >= 0) {
//				System.out.println("nlIndex " + nlIndex + " sb.length " + sb.length());
				CharSequence subSeq = sb.subSequence(0, nlIndex);
				sb.delete(0, nlIndex+1);
//				System.out.println("post cut sb.length " + sb.length());
//				sb = new StringBuilder().append(sb.substring(nlIndex + 1));
				return subSeq;
			}
//			if (numRead <= 0) {
//				return null;
//			}
			buffer.flip();
			decoder.decode(buffer, charBuffer, true);
			charBuffer.flip();
			sb.append(charBuffer);
//			System.out.print(sb);
			done = true;
			return sb;
		}

		@Override
		@Suspendable
		public void close() {
			try {
				getChannel().close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		@Suspendable
		public FiberFileChannel getChannel() {
			if (channel == null) { 
				try {
					this.channel = FiberFileChannel.open(path);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			return channel;
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
		try (LineSourceWrapper reader = new LineSourceWrapper(new BufferedReader(new InputStreamReader(new FileInputStream(testFile), charset)))) {
			Integer count = new CountWordsForkTask(reader, batchSize).fork().join();
			cachedResult.compareAndSet(0, (int) count);
			if (cachedResult.get() != count) {
				throw new AssertionError();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	
	@SuppressWarnings("serial")
	public static class CountWordsFiber extends Fiber<Integer>{
		CountWordsTask task;
		private Path path;
		private Charset cs;
		private int batchSize;
		public CountWordsFiber(Path path, Charset cs, int batchSize) {
			this.path = path;
			this.cs = cs;
			this.batchSize = batchSize;
		}
		
		public CountWordsFiber(LineSourceI reader, int batchSize) {
			task = new CountWordsTask(reader, batchSize) {
				Fiber<Integer> nextTask;
				@Override
				protected void startNextTast(LineSourceI reader, int batchSize) {
					nextTask = new CountWordsFiber(reader, batchSize).start();
				}
				
				@Override
				protected int getNextTaskResult() throws ExecutionException, InterruptedException {
					return nextTask.get();
				}
			};
		}

		@Override
		@Suspendable
		public Integer run() throws SuspendExecution, InterruptedException {
			if (task == null) {
				task = new CountWordsTask(new LineSource(path, cs), batchSize) {
					Fiber<Integer> nextTask;
					@Override
					protected void startNextTast(LineSourceI reader, int batchSize) {
						nextTask = new CountWordsFiber(reader, batchSize).start();
					}
					
					@Override
					protected int getNextTaskResult() throws ExecutionException, InterruptedException {
						return nextTask.get();
					}
				};
			}
			return task.work();
		}
	}
	
	@SuppressWarnings("serial")
	public static class CountWordsForkTask extends RecursiveTask<Integer>{
		CountWordsTask task;
		public CountWordsForkTask(LineSourceI reader, int batchSize) {
			task = new CountWordsTask(reader, batchSize) {
				ForkJoinTask<Integer> nextTask;
				@Override
				protected void startNextTast(LineSourceI reader, int batchSize) {
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
		private LineSourceI reader;
		private int batchSize;
		
		public CountWordsTask(LineSourceI reader, int batchSize) {
			this.reader = reader;
			this.batchSize = batchSize;
		}
		
		protected Integer work() throws SuspendExecution {
			try {
				ArrayDeque<CharSequence> deque = new ArrayDeque<>();
				CharSequence line = null;
				while ((line = reader.readLine())!=null && deque.size() < batchSize) {
					deque.add(line);
				}
				if (deque.size() == 0) {
					return 0;
				}
				startNextTast(reader, batchSize);
				int count = 0;
				for (CharSequence currentLine : deque) {
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
		protected abstract void startNextTast(LineSourceI reader, int batchSize); //	ForkJoinTask<Integer> nextTask = new CountWorksTask(reader, batchSize).fork();
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
