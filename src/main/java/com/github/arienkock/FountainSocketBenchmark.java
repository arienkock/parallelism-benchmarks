package com.github.arienkock;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import org.openjdk.jmh.annotations.*;

import co.paralleluniverse.fibers.*;
import co.paralleluniverse.fibers.io.FiberServerSocketChannel;
import co.paralleluniverse.fibers.io.FiberSocketChannel;
import co.paralleluniverse.strands.SuspendableRunnable;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Benchmark)
public class FountainSocketBenchmark {
	private ExecutorService executorService = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setDaemon(true).build());
	private AsynchronousChannelGroup asynChanGroup;
	@Param({"10000"})
	private int BYTE_COUNT_LIMIT;
	@Param({"64"})
	private int NUM_CLIENTS;
	@Param({"1024"})
	private int READ_BUFFER_SIZE;
	@Param({"8"})
	private int SERVER_COUNT;

	@Benchmark
	public void fountainFibers() {
		List<Fiber<Void>> fibers = new ArrayList<>();
		for (int count = 0; count < SERVER_COUNT; count++) {
			Fiber<Void> f = new Fiber<Void>(serverRunnableFiber(BYTE_COUNT_LIMIT, NUM_CLIENTS))
					.start();
			fibers.add(f);
		}
		fibers.forEach((f)->{
			try {
				f.join();
			} catch (ExecutionException | InterruptedException e) {
				e.printStackTrace();
			}
		});
	}

	public SuspendableRunnable serverRunnableFiber(int byteCount, int numClients) {
		return new SuspendableRunnable() {
			@Override
			public void run() throws SuspendExecution, InterruptedException {
				try (FiberServerSocketChannel socket = FiberServerSocketChannel
						.open(asynChanGroup).bind(null)) {
					SocketAddress localAddress = socket.getLocalAddress();
					startClientsFiber(localAddress, numClients);
					for (int count = 0; count < numClients; count++) {
						FiberSocketChannel ch = socket.accept();
						writeStuffFiber(ch, byteCount);
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		};
	}

	public void writeStuffFiber(FiberSocketChannel ch, int byteCount) {
		new Fiber<Void>(new SuspendableRunnable() {
			@Override
			public void run() throws SuspendExecution, InterruptedException {
				ByteBuffer buf = ByteBuffer.wrap("Fountain water!\n".getBytes());
				try {
					int bytesWritten = 0;
					while (bytesWritten < byteCount) {
						while (buf.remaining() > 0) {
							bytesWritten += ch.write(buf);
						}
						buf.clear();
					}
				} catch (IOException e) {
					throw new RuntimeException(e);
				} finally {
					try {
						ch.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}).start();
	}

	public void startClientsFiber(SocketAddress localAddress, int numClients) {
		for (int count = 0; count < numClients; count++) {
			new Fiber<Void>(clientRunnableFiber(localAddress)).start();
		}
	}

	private SuspendableRunnable clientRunnableFiber(SocketAddress localAddress) {
		return new SuspendableRunnable() {
			@Override
			public void run() throws SuspendExecution, InterruptedException {
				ByteBuffer buf = ByteBuffer.allocate(READ_BUFFER_SIZE);
				InetSocketAddress inetSocketAddress = new InetSocketAddress(
						"127.0.0.1",
						((InetSocketAddress) localAddress).getPort());
				try (FiberSocketChannel ch = FiberSocketChannel
						.open(asynChanGroup, inetSocketAddress)) {
					while (ch.read(buf) >= 0) {
						buf.hashCode();
						buf.clear();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		};
	}

	@Setup
	public void init() throws IOException {
		System.out.println("Setup");
		if (asynChanGroup == null) {
//			executorService = Executors.newFixedThreadPool(100);
			asynChanGroup = AsynchronousChannelGroup.withThreadPool((ExecutorService) DefaultFiberScheduler.getInstance().getExecutor());//AsynchronousChannelGroup.withThreadPool(executorService);
		}
	}
	@TearDown
	public void close() throws IOException {
//		if (asynChanGroup != null) {
//			asynChanGroup.shutdown();
//			try {
//				asynChanGroup.awaitTermination(10, TimeUnit.SECONDS);
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			} finally {
//				executorService = null;
//				asynChanGroup = null;
//			}
//		}
	}

	@Benchmark
	public void fountainThread() {
		List<Future<?>> fibers = new ArrayList<>();
		for (int count = 0; count < SERVER_COUNT; count++) {
			Future<?> f = executorService.submit(serverRunnableThread(BYTE_COUNT_LIMIT, NUM_CLIENTS));
			fibers.add(f);
		}
		for (Future<?> f : fibers) {
			try {
				f.get();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		}
	}

	public Runnable serverRunnableThread(int byteCount, int numClients) {
		return new Runnable() {
			@Override
			public void run() {
				try (ServerSocketChannel socket = ServerSocketChannel.open()
						.bind(null)) {
					SocketAddress localAddress = socket.getLocalAddress();
					startClientsThread(localAddress, numClients);
					for (int count = 0; count < numClients; count++) {
						SocketChannel ch = socket.accept();
						writeStuffThread(ch, byteCount);
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		};
	}

	public void writeStuffThread(SocketChannel ch, int byteCount) {
		executorService.submit(new Runnable() {
			@Override
			public void run() {
				ByteBuffer buf = ByteBuffer.wrap("Fountain water!\n".getBytes());
				try {
					int bytesWritten = 0;
					while (bytesWritten < byteCount) {
						while (buf.remaining() > 0) {
							bytesWritten += ch.write(buf);
						}
						buf.clear();
					}
				} catch (IOException e) {
					throw new RuntimeException(e);
				} finally {
					try {
						ch.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		});
	}

	public void startClientsThread(SocketAddress localAddress, int numClients) {
		for (int count = 0; count < numClients; count++) {
			executorService.submit(clientRunnableThread(localAddress));
		}
	}

	private Runnable clientRunnableThread(SocketAddress localAddress) {
		return new Runnable() {

			@Override
			public void run() {
				ByteBuffer buf = ByteBuffer.allocate(READ_BUFFER_SIZE);
				InetSocketAddress inetSocketAddress = new InetSocketAddress(
						"127.0.0.1",
						((InetSocketAddress) localAddress).getPort());
				try (SocketChannel ch = SocketChannel.open(inetSocketAddress)) {
					while (ch.read(buf) >= 0) {
						buf.clear();
						buf.hashCode();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		};
	}
}
