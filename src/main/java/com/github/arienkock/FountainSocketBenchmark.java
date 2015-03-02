package com.github.arienkock;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import co.paralleluniverse.fibers.DefaultFiberScheduler;
import co.paralleluniverse.fibers.Fiber;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.fibers.io.FiberServerSocketChannel;
import co.paralleluniverse.fibers.io.FiberSocketChannel;
import co.paralleluniverse.strands.SuspendableRunnable;

import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 15, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value=1, jvmArgsAppend = "-Djmh.stack.lines=5")
@State(Scope.Benchmark)
public class FountainSocketBenchmark {
	private ExecutorService executorService;
	private AsynchronousChannelGroup asynChanGroupFibers;
	private AsynchronousChannelGroup asynChanGroupThread;
	private AsynchronousChannelGroup asynChanGroupFJP;
	@Param({"20480"})
	private int BYTE_COUNT_LIMIT;
	@Param({"32"})
	private int NUM_CLIENTS;
	@Param({"1024"})
	private int READ_BUFFER_SIZE;
	@Param({"32"})
	private int SERVER_COUNT;
	private AtomicInteger monitor = new AtomicInteger();
	private CompletableFuture asyncCompletableFuture;
	private AtomicInteger asyncMonitor;

	@Setup
	public void init() throws IOException {
		System.out.println("Setup");
		monitor.set(0);
		if (executorService == null) {
			executorService = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setDaemon(true).build());
		}
		if (asynChanGroupFibers == null) {
			asynChanGroupFibers = AsynchronousChannelGroup.withThreadPool((ExecutorService) DefaultFiberScheduler.getInstance().getExecutor());
		}
		if (asynChanGroupFJP == null) {
			asynChanGroupFJP = AsynchronousChannelGroup.withThreadPool(ForkJoinPool.commonPool());
		}
	}
	@TearDown
	public void close() throws IOException {
		try {asynChanGroupFibers.shutdownNow();} catch (Throwable t) {} finally {asynChanGroupFibers=null;}
		try {asynChanGroupThread.shutdownNow();} catch (Throwable t) {} finally {asynChanGroupThread=null;}
		try {executorService.shutdownNow();} catch (Throwable t) {} finally {executorService=null;}
		System.out.println("Bytes read: " + monitor.get());
	}

//	@Benchmark
	public void fountainFibers() {
		List<Fiber<Void>> fibers = new ArrayList<>();
		for (int count = 0; count < SERVER_COUNT; count++) {
			fibers.add(new Fiber<Void>(serverRunnableFiber(BYTE_COUNT_LIMIT, NUM_CLIENTS)).start());
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
						.open(null).bind(null)) {
					SocketAddress localAddress = socket.getLocalAddress();
					List<Fiber<Void>> clientsFibers = startClientsFiber(localAddress, numClients);
					List<Fiber<Void>> fibers = new ArrayList<Fiber<Void>>();
					for (int count = 0; count < numClients; count++) {
						FiberSocketChannel ch = socket.accept();
						fibers.add(writeStuffFiber(ch, byteCount));
					}
					for (Fiber<Void> fiber : Iterables.concat(clientsFibers,fibers)) {
						try {
							fiber.join();
						} catch (ExecutionException e) {
							e.printStackTrace();
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		};
	}

	public Fiber<Void> writeStuffFiber(FiberSocketChannel ch, int byteCount) {
		return new Fiber<Void>(new SuspendableRunnable() {
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

	public List<Fiber<Void>> startClientsFiber(SocketAddress localAddress, int numClients) {
		List<Fiber<Void>> fibers = new ArrayList<Fiber<Void>>();
		for (int count = 0; count < numClients; count++) {
			fibers.add(new Fiber<Void>(clientRunnableFiber(localAddress)).start());
		}
		return fibers;
	}

	private SuspendableRunnable clientRunnableFiber(SocketAddress localAddress) {
		return new SuspendableRunnable() {
			@Override
			public void run() throws SuspendExecution, InterruptedException {
				ByteBuffer buf = ByteBuffer.allocate(READ_BUFFER_SIZE);
				InetSocketAddress inetSocketAddress = null;
				try {
					inetSocketAddress = new InetSocketAddress(
							InetAddress.getLocalHost(),
							((InetSocketAddress) localAddress).getPort());
				} catch (UnknownHostException e1) {
					Throwables.propagate(e1);
				}
				try (FiberSocketChannel ch = FiberSocketChannel
						.open(asynChanGroupFibers, inetSocketAddress)) {
					int n = 0;
					while ((n=ch.read(buf)) >= 0) {
						monitor.getAndAdd(n);
						buf.hashCode();
						buf.clear();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		};
	}

//	@Benchmark
	public void fountainThread() {
		List<Future<?>> fibers = new ArrayList<>();
		for (int count = 0; count < SERVER_COUNT; count++) {
			fibers.add(executorService.submit(serverRunnableThread(BYTE_COUNT_LIMIT, NUM_CLIENTS)));
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
					List<Future<?>> clientFutures = startClientsThread(localAddress, numClients);
					List<Future<?>> futures = new ArrayList<Future<?>>();
					for (int count = 0; count < numClients; count++) {
						SocketChannel ch = socket.accept();
						futures.add(writeStuffThread(ch, byteCount));
					}
					for (Future<?> fiber : Iterables.concat(clientFutures,futures)) {
						try {
							fiber.get();
						} catch (ExecutionException | InterruptedException e) {
							e.printStackTrace();
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		};
	}

	public Future<?> writeStuffThread(SocketChannel ch, int byteCount) {
		return executorService.submit(new Runnable() {
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

	public List<Future<?>> startClientsThread(SocketAddress localAddress, int numClients) {
		List<Future<?>> futures = new ArrayList<Future<?>>();
		for (int count = 0; count < numClients; count++) {
			futures.add(executorService.submit(clientRunnableThread(localAddress)));
		}
		return futures;
	}

	private Runnable clientRunnableThread(SocketAddress localAddress) {
		return new Runnable() {

			@Override
			public void run() {
				ByteBuffer buf = ByteBuffer.allocate(READ_BUFFER_SIZE);
				InetSocketAddress inetSocketAddress = null;
				try {
					inetSocketAddress = new InetSocketAddress(
							InetAddress.getLocalHost(),
							((InetSocketAddress) localAddress).getPort());
				} catch (UnknownHostException e1) {
					Throwables.propagate(e1);
				}
				try (SocketChannel ch = SocketChannel.open(inetSocketAddress)) {
					int n=0;
					while ((n=ch.read(buf)) >= 0) {
						monitor.getAndAdd(n);
						buf.hashCode();
						buf.clear();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		};
	}
	
	@Benchmark
	public void fountainThreadAsync() throws InterruptedException, ExecutionException {
		asyncCompletableFuture  = new CompletableFuture();
		asyncMonitor = new AtomicInteger(SERVER_COUNT);
		List<ForkJoinTask<?>> futures = new ArrayList<ForkJoinTask<?>>();
		for (int count = 0; count < SERVER_COUNT; count++) {
			futures.add(ForkJoinPool.commonPool().submit(serverAsyncRunnable(BYTE_COUNT_LIMIT, NUM_CLIENTS)));
		}
		asyncCompletableFuture.get();
	}
	
	public Runnable serverAsyncRunnable(int byteCount, int numClients) {
		return new Runnable() {
			@Override
			public void run() {
				try {
					AsynchronousServerSocketChannel socket = AsynchronousServerSocketChannel.open(asynChanGroupFJP)
							.bind(null);
//					System.out.println("bound");
					SocketAddress localAddress = socket.getLocalAddress();
					List<ForkJoinTask<?>> startClientsThreadAsync = startClientsThreadAsync(localAddress, numClients);
					socket.accept(numClients, acceptCompletionHandler(byteCount, socket));
					for (ForkJoinTask<?> f:startClientsThreadAsync) {
						f.join();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

		};
	}
	
	private CompletionHandler<AsynchronousSocketChannel, Integer> acceptCompletionHandler(
			int byteCount, AsynchronousServerSocketChannel socket) {
		return new CompletionHandler<AsynchronousSocketChannel, Integer>() {
			public void completed(
					AsynchronousSocketChannel ch,
					Integer acceptsToGo) {
				acceptsToGo = acceptsToGo-1;
//				System.out.println("server accepted, to go = " + acceptsToGo);
				writeStuffThreadAsync(socket, ch, byteCount, acceptsToGo);
				if (acceptsToGo > 0) {
					socket.accept(acceptsToGo, acceptCompletionHandler(byteCount, socket));
				}
			}
			public void failed(Throwable exc, Integer attachment) {
				exc.printStackTrace();
				try {
					socket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		};
	}

	private void writeStuffThreadAsync(
			AsynchronousServerSocketChannel socket, AsynchronousSocketChannel ch, int byteCount, int countDown) {
		ByteBuffer buf = ByteBuffer.wrap("Fountain water!\n".getBytes());
		ch.write(buf, 0, writeCompletionHandler(socket, ch, byteCount, buf, countDown));
	}
	
	private CompletionHandler<Integer, Integer> writeCompletionHandler(
			AsynchronousServerSocketChannel socket, AsynchronousSocketChannel ch, int byteCount, ByteBuffer buf, int countDown) {
		return new CompletionHandler<Integer, Integer>() {

			@Override
			public void completed(Integer result, Integer total) {
				total += result;
				if (total < byteCount) {
					buf.clear();
					ch.write(buf, total, writeCompletionHandler(socket, ch, byteCount, buf, countDown));
				} else {
					try {
						ch.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
//					System.out.println("count down " + countDown);
					if (countDown == 0) {
						try {
							socket.close();
//							System.out.println("asyncMonitor " + asyncMonitor.get());
							if (asyncMonitor.decrementAndGet() == 0) {
								asyncCompletableFuture.complete(null);
							}
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			}

			@Override
			public void failed(Throwable exc, Integer total) {
				exc.printStackTrace();
				try {
					ch.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		};
	}
	
	private List<ForkJoinTask<?>> startClientsThreadAsync(
			SocketAddress localAddress, int numClients) {
		InetSocketAddress inetSocketAddress = null;
		try {
			inetSocketAddress = new InetSocketAddress(
					InetAddress.getLocalHost(),
					((InetSocketAddress) localAddress).getPort());
		} catch (UnknownHostException e1) {
			Throwables.propagate(e1);
		}
		List<ForkJoinTask<?>> fibers = new ArrayList<>();
		for (int i=0; i<numClients; i++) {
			fibers.add(ForkJoinTask.adapt(asyncClientRunnable(inetSocketAddress)).fork());
		}
		return fibers;
	}
	
	private Runnable asyncClientRunnable(InetSocketAddress inetSocketAddress) {
		ByteBuffer buf = ByteBuffer.allocate(READ_BUFFER_SIZE);
		return new Runnable() {
			@Override
			public void run() {
				try {
					AsynchronousSocketChannel ch = AsynchronousSocketChannel.open(asynChanGroupFJP);
					ch.connect(inetSocketAddress, null, new CompletionHandler<Void, Void>() {
						@Override
						public void completed(Void result, Void attachment) {
//							System.out.println("connected client");
							ch.read(buf, null, completionHandler(buf, ch));
						}
						
						@Override
						public void failed(Throwable exc, Void attachment) {
							exc.printStackTrace();
							try {
								ch.close();
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
					});
				} catch (IOException e1) {
					Throwables.propagate(e1);
				}
			}
		};
	}
	
	private CompletionHandler<Integer, Void> completionHandler(
			ByteBuffer buf, AsynchronousSocketChannel ch) {
		return new CompletionHandler<Integer, Void>() {
			@Override
			public void completed(Integer result,
					Void attachment) {
				if (result >= 0) {
					monitor.getAndAdd(result);
					buf.hashCode();
					buf.clear();
					ch.read(buf, null, completionHandler(buf, ch));
				} else {
					try {
						ch.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}

			@Override
			public void failed(Throwable exc,
					Void attachment) {
				exc.printStackTrace();
				try {
					ch.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		};
	}
}
