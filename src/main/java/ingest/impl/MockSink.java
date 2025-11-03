package ingest.impl;

import ingest.io.ByteSource;
import ingest.io.IngestSink;
import ingest.model.IngestConfig;
import ingest.model.IngestResult;
import ingest.model.UploadMeta;

import java.io.IOException;

public class MockSink implements IngestSink {
    private long totalBytesRead = 0;
    private boolean exceededMaxContentLength = false;
  private IngestResult lastResult;

    private final IngestConfig config;

    public MockSink(IngestConfig config) {
        this.config = config;
    }

    @Override
    public void persist(UploadMeta meta, IngestResult result, ByteSource src) {
        long bytesRead = 0;

        try {
          byte[] chunk;
          while ((chunk = src.nextChunk()) != null && chunk.length > 0) {
              bytesRead += chunk.length;

            // check as we go to avoid consuming huge files accidentally
              if (bytesRead > config.getMaxContentLength()) {
                  exceededMaxContentLength = true;
                  throw new IOException("Sink exceeded maxContentLength (" +
                        config.getMaxContentLength() + " bytes)");
              }
          }

          totalBytesRead = bytesRead;

          if (bytesRead != result.getSize()) {
            throw new AssertionError("Sink read " + bytesRead +
                    " bytes, but IngestResult.size = " + result.getSize());
          }

          lastResult = result;

          System.out.printf("MockSink: persisted '%s' (%d bytes, mime=%s)%n",
                  meta.getFilename(), bytesRead, result.getDetectedMime());

        } catch (IOException e) {
            throw new RuntimeException("I/O error while reading ByteSource in sink", e);
        }
    }

    public long getTotalBytesRead() {
        return totalBytesRead;
    }

    /** Returns the last IngestResult persisted by this sink. */
    public IngestResult getLastResult() {
        return lastResult;
    }
}
