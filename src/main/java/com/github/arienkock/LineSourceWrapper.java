package com.github.arienkock;

import java.io.BufferedReader;
import java.io.IOException;

import co.paralleluniverse.fibers.SuspendExecution;

public class LineSourceWrapper implements StringSourceI {
	private BufferedReader reader;

	public LineSourceWrapper(BufferedReader reader) {
		this.reader = reader;
	}

	@Override
	public void close() {
		try {
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public String readString() throws IOException, SuspendExecution {
		return reader.readLine();
	}
}