package com.foo.dedup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class FilesUtil {
    private static final Logger LOG = LoggerFactory.getLogger(FilesUtil.class);

    public static void removeDirectory(Path p, String source) throws IOException {
        if (!Files.exists(p)) {
            LOG.debug(
                    "Attempt to delete non-existent path: {}. Ensure this resource is cleaned up exactly once. Source: {}",
                    p,
                    source);
            return;
        }
        removeDirectory(p);
    }

    public static void removeDirectory(Path p) throws IOException {
        if (!Files.exists(p)) {
            LOG.debug(
                    "Attempt to delete non-existent path: {}. Ensure this resource is cleaned up exactly once.",
                    p);
            return;
        }
        Files.walk(p).map(Path::toFile).sorted((o1, o2) -> -1 * o1.compareTo(o2)).forEach(File::delete);
    }
}
