package ingest.impl;

import ingest.io.ByteSource;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * Simple ByteSource implementation that reads from a file in 4KB chunks.
 */
public class FileByteSource implements ByteSource {
  private final FileInputStream in;
  private static final int CHUNK_SIZE = 4096;

  public FileByteSource(String path) throws IOException {
    this.in = new FileInputStream(path);
  }

  @Override
  public byte[] nextChunk() throws IOException {
    byte[] buffer = new byte[CHUNK_SIZE];
    int bytesRead = in.read(buffer);
    if (bytesRead == -1) {
      in.close();
      return null;
    }
    if (bytesRead < CHUNK_SIZE) {
      byte[] smaller = new byte[bytesRead];
      System.arraycopy(buffer, 0, smaller, 0, bytesRead);
      return smaller;
    }
    return buffer;
  }
}
