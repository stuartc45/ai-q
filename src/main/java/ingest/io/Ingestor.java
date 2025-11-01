package ingest.io;

import ingest.model.IngestConfig;
import ingest.model.UploadMeta;

public interface Ingestor {
    void ingest(UploadMeta meta, IngestConfig cfg, ByteSource src, IngestSink sink) throws Exception;
}
