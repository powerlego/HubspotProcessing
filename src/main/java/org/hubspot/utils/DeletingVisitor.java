package org.hubspot.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * @author Nicholas Curl
 */
public class DeletingVisitor extends SimpleFileVisitor<Path> {

    /**
     * The instance of the logger
     */
    private static final Logger  logger = LogManager.getLogger();
    private final        boolean testMode;

    public DeletingVisitor(final boolean testMode) {
        this.testMode = testMode;
    }

    @Override
    public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
        if (isTestMode()) {
            logger.debug("Deleting {} (TEST MODE: file not actually deleted)", file);
        }
        else {
            delete(file);
        }
        return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFileFailed(Path file, IOException ioException) throws IOException {
        if (ioException instanceof NoSuchFileException) {
            logger.debug("File {} could not be accessed, it has likely already been deleted", file, ioException);
            return FileVisitResult.CONTINUE;
        }
        else {
            return super.visitFileFailed(file, ioException);
        }
    }

    @Override
    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
        if (isTestMode()) {
            logger.debug("Deleting {} (TEST MODE: directory not actually deleted)", dir);
        }
        else {
            delete(dir);
        }
        return FileVisitResult.CONTINUE;
    }

    /**
     * Returns {@code true} if files are not deleted even when all conditions accept a path, {@code false} otherwise.
     *
     * @return {@code true} if files are not deleted even when all conditions accept a path, {@code false} otherwise
     */
    public boolean isTestMode() {
        return testMode;
    }

    /**
     * Deletes the specified file. @param file the file to delete @throws IOException if a problem occurred deleting the
     * file
     */
    protected void delete(final Path file) throws IOException {
        logger.trace("Deleting {}", file);
        Files.deleteIfExists(file);
    }
}
