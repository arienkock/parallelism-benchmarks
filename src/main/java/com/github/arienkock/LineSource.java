package com.github.arienkock;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.file.Path;

import co.paralleluniverse.fibers.Suspendable;
import co.paralleluniverse.fibers.io.FiberFileChannel;

public class LineSource implements LineSourceI {
		private FiberFileChannel channel;
//		private InputStream inputStream;
		private CharsetDecoder decoder;
		private StringBuilder sb = new StringBuilder(8192);
		private ByteBuffer buffer;
		private CharBuffer charBuffer;
		private Path path;
		private boolean done = false;
		private boolean skipLF = false, ignoreLF = false;
//	    private char cb[];
	    private int nChars, nextChar;
	    private static int defaultExpectedLineLength = 80;

		public LineSource(/*FiberFileChannel channel, */Path path, Charset charset) {
			this.path = path;
//			this.inputStream = Channels.newInputStream(channel);
			this.decoder = charset.newDecoder();
			this.buffer = ByteBuffer.allocateDirect(8192);
			this.charBuffer = CharBuffer.allocate(8192);
		}
		
		@Suspendable
		public CharSequence readLine() throws IOException {
	        StringBuffer s = null;
	        int startChar;

	            boolean omitLF = ignoreLF || skipLF;

	        bufferLoop:
	            for (;;) {

	                if (nextChar >= nChars)
	                    fill();
	                if (nextChar >= nChars) { /* EOF */
	                    if (s != null && s.length() > 0)
	                        return s.toString();
	                    else
	                        return null;
	                }
	                boolean eol = false;
	                char c = 0;
	                int i;

	                /* Skip a leftover '\n', if necessary */
	                if (omitLF && (charBuffer.get(nextChar) == '\n'))
	                    nextChar++;
	                skipLF = false;
	                omitLF = false;

	            charLoop:
	                for (i = nextChar; i < nChars; i++) {
	                    c = charBuffer.get(i);
	                    if ((c == '\n') || (c == '\r')) {
	                        eol = true;
	                        break charLoop;
	                    }
	                }

	                startChar = nextChar;
	                nextChar = i;

	                if (eol) {
	                    String str;
	                    if (s == null) {
	                        str = charBuffer.toString();//new String(charBuffer. cb, startChar, i - startChar);
	                    } else {
	                        s.append(charBuffer, startChar, i - startChar);
	                        str = s.toString();
	                    }
	                    nextChar++;
	                    if (c == '\r') {
	                        skipLF = true;
	                    }
	                    return str;
	                }

	                if (s == null)
	                    s = new StringBuffer(defaultExpectedLineLength);
	                s.append(charBuffer, startChar, i - startChar);
	            }
	    }
		
		@Suspendable
		private void fill() throws IOException {
			charBuffer.clear();
			getChannel().read(buffer);			
			buffer.flip();
			decoder.decode(buffer, charBuffer, false);
			charBuffer.flip();
		}

		@Suspendable
		public CharSequence readLine2() throws IOException {
			if (done) {
				return null;
			}
			int nlIndex = -1, numRead = 0;
			while ((nlIndex = sb.indexOf("\n")) < 0 && (numRead = getChannel().read(buffer)) > 0) {
//				System.out.println("nl index = " + nlIndex + "  numRead "+ numRead + " sb.length " + sb.length());
				buffer.flip();
				decoder.decode(buffer, charBuffer, false);
				charBuffer.flip();
				sb.append(charBuffer);
//				System.out.print("Champ!"+sb);
				charBuffer.clear();
				buffer.clear();
			}
			if (nlIndex >= 0) {
//				System.out.println("nlIndex " + nlIndex + " sb.length " + sb.length());
				CharSequence subSeq = sb.subSequence(0, nlIndex);
				sb.delete(0, nlIndex+1);
//				System.out.println("post cut sb.length " + sb.length());
//				sb = new StringBuilder().append(sb.substring(nlIndex + 1));
				return subSeq;
			}
//			if (numRead <= 0) {
//				return null;
//			}
			buffer.flip();
			decoder.decode(buffer, charBuffer, true);
			charBuffer.flip();
			sb.append(charBuffer);
//			System.out.print(sb);
			done = true;
			return sb;
		}

		@Override
		@Suspendable
		public void close() {
			try {
				getChannel().close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		@Suspendable
		public FiberFileChannel getChannel() {
			if (channel == null) { 
				try {
					this.channel = FiberFileChannel.open(path);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			return channel;
		}

	}