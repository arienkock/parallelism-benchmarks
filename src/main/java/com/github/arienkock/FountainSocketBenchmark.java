package com.github.arienkock;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import org.openjdk.jmh.annotations.*;

import co.paralleluniverse.fibers.Fiber;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.fibers.io.FiberServerSocketChannel;
import co.paralleluniverse.fibers.io.FiberSocketChannel;
import co.paralleluniverse.strands.SuspendableRunnable;

@State(Scope.Benchmark)
public class FountainSocketBenchmark {
	private ExecutorService executorService;
	private AsynchronousChannelGroup asynChanGroup;

	@Benchmark
	public void fountainFibers() {
		List<Fiber<Void>> fibers = new ArrayList<>();
		for (int count = 0; count < 2; count++) {
			Fiber<Void> f = new Fiber<Void>(serverRunnableFiber(1000, 2))
					.start();
			fibers.add(f);
		}
		for (Fiber<Void> f : fibers) {
			try {
				f.join();
			} catch (ExecutionException | InterruptedException e) {
				e.printStackTrace();
			}
		}
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
				ByteBuffer buf = ByteBuffer.allocate(1024);
				InetSocketAddress inetSocketAddress = new InetSocketAddress(
						"127.0.0.1",
						((InetSocketAddress) localAddress).getPort());
				try (FiberSocketChannel ch = FiberSocketChannel
						.open(asynChanGroup, inetSocketAddress)) {
					while (ch.read(buf) >= 0) {
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
		if (executorService == null) {
			executorService = Executors.newFixedThreadPool(100);
			asynChanGroup = AsynchronousChannelGroup.withThreadPool(executorService);
		}
	}
	@TearDown
	public void close() throws IOException {
		if (executorService != null) {
			asynChanGroup.shutdown();
			try {
				asynChanGroup.awaitTermination(10, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
			} finally {
				executorService = null;
				asynChanGroup = null;
			}
		}
	}

	@Benchmark
	public void fountainThread() {
		List<Future<?>> fibers = new ArrayList<>();
		for (int count = 0; count < 2; count++) {
			Future<?> f = executorService.submit(serverRunnableThread(1000, 2));
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
				ByteBuffer buf = ByteBuffer.allocate(1024);
				InetSocketAddress inetSocketAddress = new InetSocketAddress(
						"127.0.0.1",
						((InetSocketAddress) localAddress).getPort());
				try (SocketChannel ch = SocketChannel.open(inetSocketAddress)) {
					while (ch.read(buf) >= 0) {
						buf.clear();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		};
	}
}
