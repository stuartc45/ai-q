package ingest.impl;

import ingest.io.*;
import ingest.model.*;
import ingest.util.HashUtils;

import java.io.ByteArrayOutputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

public class DefaultIngestor implements Ingestor {

    @Override
    public void ingest(UploadMeta meta, IngestConfig cfg, ByteSource src, IngestSink sink) throws Exception {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      long size = 0;
      List<String> errors = new ArrayList<>();

      // Buffer to capture up to 8KB for MIME detection
      ByteArrayOutputStream mimeBuffer = new ByteArrayOutputStream();

      // Buffer to capture *all* bytes to forward later (for sink)
      ByteArrayOutputStream forwardBuffer = new ByteArrayOutputStream();

      byte[] chunk;
      while ((chunk = src.nextChunk()) != null && chunk.length > 0) {
        size += chunk.length;
        digest.update(chunk);
        forwardBuffer.write(chunk); // store everything for sink

        // capture only first few KB for MIME detection
        if (mimeBuffer.size() < 8192) {
          mimeBuffer.write(chunk, 0, Math.min(chunk.length, 8192 - mimeBuffer.size()));
        }

        // enforce max size as we read
        if (size > cfg.getMaxContentLength()) {
          errors.add("File exceeds maxContentLength");
          break;
        }
      }

      String detectedMime = MimeDetector.detect(mimeBuffer.toByteArray());
      String sha256 = HashUtils.bytesToHex(digest.digest());

      final long finalSize = size;

      // --- validations ---
      meta.getContentLength().ifPresent(cl -> {
        if (!cl.equals(finalSize)) {
          errors.add("contentLength mismatch: " + cl + " != " + finalSize);
        }
      });

      if (!cfg.getAcceptedMimes().contains(detectedMime)) {
        errors.add("MIME not accepted: " + detectedMime);
      }

      if (!meta.getClaimedMime().equalsIgnoreCase(detectedMime)) {
        errors.add("Claimed MIME mismatch: " + meta.getClaimedMime() + " vs " + detectedMime);
      }

      boolean ok = errors.isEmpty();
      IngestResult result = new IngestResult(detectedMime, size, sha256, ok, errors);

      // --- create new ByteSource for forwarding ---
      byte[] allBytes = forwardBuffer.toByteArray();
      ByteSource forwardSource = new ByteSource() {
        private int offset = 0;
        private final int CHUNK_SIZE = 4096;

        @Override
        public byte[] nextChunk() {
          if (offset >= allBytes.length) return null;
          int remaining = allBytes.length - offset;
          int chunkSize = Math.min(CHUNK_SIZE, remaining);
          byte[] chunk = new byte[chunkSize];
          System.arraycopy(allBytes, offset, chunk, 0, chunkSize);
          offset += chunkSize;
          return chunk;
        }
      };

      // --- forward to sink ---
      sink.persist(meta, result, forwardSource);
    }
}
