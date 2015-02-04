package com.github.arienkock;

import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;

import co.paralleluniverse.fibers.*;

@SuppressWarnings("serial")
public class CountWordsFiber extends Fiber<Integer>{
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