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

    @Override
    public void trace(String message, Object... objects) {
        logger.trace(message, objects);
    }

    @Override
    public void debug(String message, Object... objects) {
        logger.debug(message, objects);

    }

    @Override
    public void info(String message, Object... objects) {
        logger.info(message, objects);

    }

    @Override
    public void warn(String message, Object... objects) {
        logger.warn(message, objects);

    }

    @Override
    public void error(String message, Object... objects) {
        logger.error(message, objects);

    }
    public void error(String message, Exception e) {
        logger.error(message, e);
    }


}
