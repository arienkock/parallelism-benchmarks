package com.github.arienkock;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.fibers.Suspendable;

public class ByteChannelLineSource implements StringSourceI {
	private ReadableByteChannel channel;
	private CharsetDecoder decoder;
	private ByteBuffer buffer;
	private CharBuffer charBuffer;
	private boolean done = false;

	public ByteChannelLineSource(ReadableByteChannel channel, Charset charset,
			int bufferSize) {
		this.decoder = charset.newDecoder();
		this.buffer = ByteBuffer.allocateDirect(bufferSize);
		this.charBuffer = CharBuffer.allocate(bufferSize);
		this.channel = channel;
	}

	public String readString() throws IOException, SuspendExecution {
		if (done) {
			return null;
		}
		if (done = (channel.read(buffer) < 0)) {
			return null;
		}
		buffer.flip();
		charBuffer.clear();
		decoder.decode(buffer, charBuffer, false);
		buffer.compact();
		charBuffer.flip();
		return charBuffer.toString();
	} 

	@Override
	@Suspendable
	public void close() {
		try {
			channel.close();
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
	//
	// @Suspendable
	// public FiberFileChannel getChannel() {
	// if (channel == null) {
	// try {
	// this.channel = FiberFileChannel.open(path);
	// } catch (IOException e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// }
	// return channel;
	// }

}