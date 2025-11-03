package ingest;

import ingest.model.UploadMeta;
import ingest.model.IngestConfig;
import ingest.model.IngestResult;
import ingest.impl.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URL;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class IngestorTests {

  private DefaultIngestor ingestor;
  private IngestConfig config;

  @BeforeEach
  void setup() {
    ingestor = new DefaultIngestor();
    config = new IngestConfig(
            10_000_000L, // 10 MB
            new HashSet<>(Arrays.asList(
                    "application/pdf",
                    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    "image/png"))
    );
  }

  /** Helper: get test file from /test/resources folder */
  private File getResourceFile(String name) {
    URL resource = getClass().getClassLoader().getResource(name);
    assertNotNull(resource, "Missing test resource: " + name);
    return new File(resource.getFile());
  }

  /** Helper to run ingest and return result */
  private IngestResult runIngest(String filename, String claimedMime, Long contentLength, long maxContentLength) throws Exception {
    File file = getResourceFile(filename);
    UploadMeta meta = new UploadMeta(filename, claimedMime, Optional.ofNullable(contentLength));
    IngestConfig cfg = new IngestConfig(maxContentLength, config.getAcceptedMimes());

    FileByteSource src = new FileByteSource(file.getAbsolutePath());
    MockSink sink = new MockSink(cfg);

    ingestor.ingest(meta, cfg, src, sink);

    IngestResult result = sink.getLastResult();
    assertNotNull(result, "Sink should receive result");
    assertEquals(sink.getTotalBytesRead(), result.getSize(), "Sink should consume exactly result.size bytes");

    return result;
  }

  // ---------------- HAPPY PATHS ----------------

  @Test
  void testPdfHappyPath() throws Exception {
    IngestResult result = runIngest("sample.pdf", "application/pdf", null, config.getMaxContentLength());
    assertTrue(result.isOk(), "PDF should pass validation");
    assertEquals("application/pdf", result.getDetectedMime());
  }

  @Test
  void testDocxHappyPath() throws Exception {
    IngestResult result = runIngest("sample.docx",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            null, config.getMaxContentLength());
    assertTrue(result.isOk());
    assertEquals("application/vnd.openxmlformats-officedocument.wordprocessingml.document", result.getDetectedMime());
  }

  @Test
  void testPngHappyPath() throws Exception {
    IngestResult result = runIngest("sample.png", "image/png", null, config.getMaxContentLength());
    assertTrue(result.isOk());
    assertEquals("image/png", result.getDetectedMime());
  }

  // ---------------- NEGATIVE TESTS ----------------

  @Test
  void testWrongClaimedMimeButAccepted() throws Exception {
    IngestResult result = runIngest("sample.pdf", "image/png", null, config.getMaxContentLength());
    assertTrue(result.getDetectedMime().equals("application/pdf"));
    assertTrue(result.getErrors().stream().anyMatch(e -> e.contains("Claimed MIME mismatch")));
    assertTrue(result.isOk(), "Still OK since detected MIME is accepted");
  }

  @Test
  void testContentLengthOffByOne() throws Exception {
    File file = getResourceFile("sample.png");
    long len = file.length();
    IngestResult result = runIngest("sample.png", "image/png", len + 1, config.getMaxContentLength());
    assertFalse(result.isOk());
    assertTrue(result.getErrors().stream().anyMatch(e -> e.contains("contentLength mismatch")));
  }

  @Test
  void testExceedsMaxContentLength() throws Exception {
    // force a very small limit
    IngestResult result = runIngest("sample.pdf", "application/pdf", null, 1000L);
    assertFalse(result.isOk());
    assertTrue(result.getErrors().stream().anyMatch(e -> e.contains("maxContentLength")));
  }

  // ---------------- EDGE CASES ----------------

  @Test
  void testNoContentLengthProvided() throws Exception {
    IngestResult result = runIngest("sample.png", "image/png", null, config.getMaxContentLength());
    assertTrue(result.isOk(), "Should still work without contentLength");
  }

  @Test
  void testEmptyFile() throws Exception {
    File empty = getResourceFile("empty.txt");
    IngestResult result = runIngest("empty.txt", "text/plain", 0L, config.getMaxContentLength());
    assertFalse(result.isOk(), "Empty file should not be OK");
    assertFalse(result.getErrors().isEmpty());
  }
}