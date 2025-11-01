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
    ByteArrayOutputStream mimeBuffer = new ByteArrayOutputStream();

    byte[] chunk;
    while ((chunk = src.nextChunk()) != null && chunk.length > 0) {
      size += chunk.length;
      digest.update(chunk);
      if (mimeBuffer.size() < 8192) {
        mimeBuffer.write(chunk, 0, Math.min(chunk.length, 8192 - mimeBuffer.size()));
      }
      if (size > cfg.getMaxContentLength()) {
        errors.add("File exceeds maxContentLength");
        break;
      }
    }

    String detectedMime = MimeDetector.detect(mimeBuffer.toByteArray());
    String sha256 = HashUtils.bytesToHex(digest.digest());

    // validations
    meta.getContentLength().ifPresent(cl -> {
      if (!cl.equals(size)) errors.add("contentLength mismatch: " + cl + " != " + size);
    });

    if (!cfg.getAcceptedMimes().contains(detectedMime)) {
      errors.add("MIME not accepted: " + detectedMime);
    }

    if (!meta.getClaimedMime().equalsIgnoreCase(detectedMime)) {
      errors.add("Claimed MIME mismatch: " + meta.getClaimedMime() + " vs " + detectedMime);
    }

    boolean ok = errors.isEmpty();
    IngestResult result = new IngestResult(detectedMime, size, sha256, ok, errors);

    sink.persist(meta, result, src);
  }
}
