package com.toddfast.mutagen.cassandra.commandline;

import com.toddfast.mutagen.cassandra.util.logging.Log;

/**
 * Wrapper around a simple Console output.
 */
public class ConsoleLog implements Log {
    public static enum Level {
        DEBUG, INFO, WARN
    }

    private final Level level;

    /**
     * Creates a new Console Log.
     *
     * @param level
     *            the log level.
     */
    public ConsoleLog(Level level) {
        this.level = level;
    }

    @Override
    public void debug(String message, Object... objects) {
        System.out.println("DEBUG: " + replacedMessage(message, objects));

    }

    @Override
    public void info(String message, Object... objects) {
        System.out.println("INFO: " + replacedMessage(message, objects));

    }

    @Override
    public void warn(String message, Object... objects) {
        System.out.println("ERROR: " + replacedMessage(message, objects));

    }

    @Override
    public void error(String message, Object... objects) {
        System.out.println("ERROR: " + replacedMessage(message, objects));

    }


    public void error(String message, Exception e) {
        System.out.println("ERROR: " + replacedMessage(message, e));
    }

    @Override
    public void trace(String message, Object... objects) {
        System.out.println("TRACE: " + replacedMessage(message, objects));
    }

    private String replacedMessage(String message, Object... objects) {
        String localMessage = message;
        for (Object object : objects) {
            localMessage = localMessage.replaceFirst("\\{\\}", "{ " + object.toString() + " }");
        }
        return localMessage;
    }
}
