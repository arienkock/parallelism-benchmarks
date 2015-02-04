package com.github.arienkock;

import java.io.IOException;

public interface LineSourceI extends AutoCloseable {
	public CharSequence readLine() throws IOException;
	public void close();
}