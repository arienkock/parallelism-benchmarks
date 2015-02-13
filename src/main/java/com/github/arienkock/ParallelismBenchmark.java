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
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.RandomStringUtils;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import co.paralleluniverse.fibers.Fiber;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.fibers.io.FiberFileChannel;
import co.paralleluniverse.strands.SuspendableRunnable;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

@SuppressWarnings("serial")
@State(Scope.Benchmark)
public class ParallelismBenchmark {

	private static final int IO_TOKENS = 10;
	private static final int COMPUTE_TOKENS = 1_000_000;
	private Charset charset = Charset.forName("UTF-8");
	private File testFile = new File(Paths.get(System.getProperty("user.dir"))
			.toFile(), "parallelismbenchmarktestdata.txt");
	public static final int BUFFER_SIZE = 8000;

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
	
	AtomicInteger monitor = new AtomicInteger(0);
	
	@Setup
	public void init() {
	}

	
	SuspendableRunnable ioRunnableF = new SuspendableRunnable() {
		@Override
		public void run() throws SuspendExecution {
			try (FiberFileChannel ch = FiberFileChannel.open(fiberFileThreadPool, testFile.toPath(), Collections.EMPTY_SET)) {
				ByteBuffer dst = ByteBuffer.allocateDirect(BUFFER_SIZE);
				for (int read = 0; read >= 0; read = ch.read(dst)) {
					Blackhole.consumeCPU(IO_TOKENS);
					dst.hashCode();
					dst.clear();
				}
				monitor.getAndIncrement();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	};

	SuspendableRunnable computeRunnableF = new SuspendableRunnable() {
		@Override
		public void run() throws SuspendExecution {
			Blackhole.consumeCPU(COMPUTE_TOKENS);
			monitor.getAndIncrement();
		}
	};

	@Benchmark
	public void testFiberChannel() throws IOException, Exception {
		monitor.set(0);
		for (int i = 0; i < 10; i++) {
			new Fiber<Void>(ioRunnableF).start();
		}
//		for (int i = 0; i < 10; i++) {
//			new Fiber<Void>(computeRunnableF).start();
//		}
//		MonitoredForkJoinPool executor = (MonitoredForkJoinPool)DefaultFiberScheduler.getInstance().getExecutor();
		while (monitor.get() < 10) {
			Thread.yield();
		}
	}
	
    private static final ExecutorService fiberFileThreadPool = Executors.newFixedThreadPool(10,
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("blala").build());

	Runnable ioRunnableT = new Runnable() {
		@Override
		public void run() {
			try {
				AsynchronousFileChannel ch = AsynchronousFileChannel
						.open(testFile.toPath(), Collections.EMPTY_SET, fiberFileThreadPool);
				ByteBuffer dst = ByteBuffer.allocateDirect(BUFFER_SIZE);
				doRead(ch, dst);
			} catch (Throwable t) {
				throw new RuntimeException(t);
			}
		}

	};
	private void doRead(AsynchronousFileChannel ch, ByteBuffer dst) {
		ch.read(dst, dst.position(), null,
				new CompletionHandler<Integer, Void>() {
			
			@Override
			public void completed(Integer result, Void attachment) {
				if (result >= 0) {
					Blackhole.consumeCPU(IO_TOKENS);
					dst.hashCode();
					dst.clear();
					doRead(ch, dst);
				} else {
					try {
						ch.close();
					} catch (IOException e) {
					}
				}
			}
			
			@Override
			public void failed(Throwable exc, Void attachment) {
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
		for (int i = 0; i < 10; i++) {
			ForkJoinTask.adapt(ioRunnableT).fork();
		}
//		for (int i = 0; i < 10; i++) {
//			ForkJoinTask.adapt(computeRunnableT).fork();
//		}
		ForkJoinPool.commonPool().awaitQuiescence(1, TimeUnit.MINUTES);
	}
}
