package com.github.arienkock;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import co.paralleluniverse.fibers.SuspendExecution;

public abstract class CountWordsTask {
	private static Pattern whitespace = Pattern.compile("[\\s]+");
	private StringSourceI reader;
	private int batchSize;
	
	public CountWordsTask(StringSourceI reader, int batchSize) {
		this.reader = reader;
		this.batchSize = batchSize;
	}
	
	protected Integer work() throws SuspendExecution {
		try {
			ArrayDeque<CharSequence> deque = new ArrayDeque<>();
			CharSequence line = null;
			while ((line = reader.readString())!=null && deque.size() < batchSize) {
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
	protected abstract void startNextTast(StringSourceI reader, int batchSize); //	ForkJoinTask<Integer> nextTask = new CountWorksTask(reader, batchSize).fork();
}