package com.toddfast.mutagen.cassandra.util.logging.slf4j;

import org.slf4j.Logger;

import com.toddfast.mutagen.cassandra.util.logging.Log;

public class Slf4jLog implements Log {
    /**
     * Slf4j Logger.
     */
    private final Logger logger;

    /**
     * Creates a new wrapper around this logger.
     *
     * @param logger
     *            The original Slf4j Logger.
     */
    public Slf4jLog(Logger logger) {
        this.logger = logger;
    }

    public void debug(String message) {
        logger.debug(message);
    }

    public void info(String message) {
        logger.info(message);
    }

    public void warn(String message) {
        logger.warn(message);
    }

    public void error(String message) {
        logger.error(message);
    }

    public void error(String message, Exception e) {
        logger.error(message, e);
    }
}
