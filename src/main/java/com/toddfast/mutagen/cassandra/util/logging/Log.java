package com.toddfast.mutagen.cassandra.util.logging;

/**
 * A logger.
 */
public interface Log {
    /**
     * Logs a debug message.
     *
     * @param message
     *            The message to log.
     */
    void debug(String message, Object... objects);

    /**
     * Logs a debug message.
     * 
     * @param message
     *            The message to log.
     */
    void trace(String message, Object... objects);

    /**
     * Logs an info message.
     * 
     * @param message
     *            The message to log.
     */
    void info(String message, Object... objects);

    /**
     * Logs a warning message.
     *
     * @param message
     *            The message to log.
     */
    void warn(String message, Object... objects);

    /**
     * Logs an error message.
     *
     * @param message
     *            The message to log.
     */
    void error(String message, Object... objects);

    /**
     * Logs an error message and the exception that caused it.
     *
     * @param message
     *            The message to log.
     * @param e
     *            The exception that caused the error.
     */
    void error(String message, Exception e);
}
