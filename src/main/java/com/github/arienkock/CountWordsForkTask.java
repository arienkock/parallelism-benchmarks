package com.github.arienkock;

import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;

@SuppressWarnings("serial")
public class CountWordsForkTask extends RecursiveTask<Integer>{
	CountWordsTask task;
	public CountWordsForkTask(StringSourceI reader, int batchSize) {
		task = new CountWordsTask(reader, batchSize) {
			ForkJoinTask<Integer> nextTask;
			@Override
			protected void startNextTast(StringSourceI reader, int batchSize) {
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
		} catch (Throwable e) {
			throw new AssertionError();
		}
	}
}