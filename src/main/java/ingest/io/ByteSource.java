package ingest.io;

public interface ByteSource {
    /**
     * Returns the next chunk of bytes, or null/empty if EOF.
     * Throws IOException on error.
     */
    byte[] nextChunk() throws java.io.IOException;
}
