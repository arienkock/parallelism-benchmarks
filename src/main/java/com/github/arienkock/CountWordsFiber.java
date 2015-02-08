package com.github.arienkock;

import java.util.concurrent.ExecutionException;

import co.paralleluniverse.fibers.Fiber;
import co.paralleluniverse.fibers.SuspendExecution;

@SuppressWarnings("serial")
public class CountWordsFiber extends Fiber<Integer>{
	private CountWordsTask task;
	
	public CountWordsFiber(StringSourceI reader, int batchSize) {
		task = new CountWordsTask(reader, batchSize) {
			Fiber<Integer> nextTask;
			@Override
			protected void startNextTast(StringSourceI reader, int batchSize) {
				nextTask = new CountWordsFiber(reader, batchSize).start();
			}
			
			@Override
			protected int getNextTaskResult() throws ExecutionException, InterruptedException {
				return nextTask.get();
			}
		};
	}

	@Override
	protected Integer run() throws SuspendExecution, InterruptedException {
		return task.work();
	}
	
}