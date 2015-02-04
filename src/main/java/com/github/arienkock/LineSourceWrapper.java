package com.github.arienkock;

import java.io.BufferedReader;
import java.io.IOException;

public class LineSourceWrapper implements LineSourceI {
	private BufferedReader reader;

	public LineSourceWrapper(BufferedReader reader) {
		this.reader = reader;
	}
	@Override
	public CharSequence readLine() throws IOException {
		return reader.readLine();
	}

	@Override
	public void close() {
		try {
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}