package com.github.arienkock;

import java.io.IOException;

import co.paralleluniverse.fibers.SuspendExecution;

public interface StringSourceI extends AutoCloseable {
	public String readString() throws IOException, SuspendExecution;
	public void close();
}