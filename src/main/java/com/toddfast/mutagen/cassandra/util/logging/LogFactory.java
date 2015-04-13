package com.toddfast.mutagen.cassandra.util.logging;

import com.toddfast.mutagen.cassandra.util.logging.slf4j.Slf4jLogCreator;

/**
 * Factory for loggers.
 */
public class LogFactory {
    /**
     * Factory for implementation-specific loggers.
     */
    private static LogCreator logCreator;

    /**
     * Prevent instantiation.
     */
    private LogFactory() {
        // Do nothing
    }

    /**
     * @param logCreator
     *            The factory for implementation-specific loggers.
     */
    public static void setLogCreator(LogCreator logCreator) {
        LogFactory.logCreator = logCreator;
    }

    /**
     * Retrieves the matching logger for this class.
     *
     * @param clazz
     *            The class to get the logger for.
     * @return The logger.
     */
    public static Log getLog(Class<?> clazz) {
        if (logCreator == null)
            logCreator = new Slf4jLogCreator();
        return logCreator.createLogger(clazz);
    }
}
