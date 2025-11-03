package ingest.impl;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class MimeDetector {
    public static String detect(byte[] bytes) {
        if (bytes.length > 4 && bytes[0] == '%' && bytes[1] == 'P' && bytes[2] == 'D') return "application/pdf";
        if (bytes.length > 2 && bytes[0] == (byte)0x50 && bytes[1] == (byte)0x4B) return "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
        if (bytes.length > 4 && bytes[0] == (byte)0x89 && bytes[1] == 0x50) return "image/png";
        return "application/octet-stream";
    }
}
