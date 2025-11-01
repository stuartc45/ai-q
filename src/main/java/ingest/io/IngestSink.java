package ingest.io;

import ingest.model.IngestResult;
import ingest.model.UploadMeta;

public interface IngestSink {
    void persist(UploadMeta meta, IngestResult result, ByteSource data) throws Exception;
}
