package com.toddfast.mutagen.cassandra.commandline;

import com.toddfast.mutagen.cassandra.commandline.ConsoleLog.Level;
import com.toddfast.mutagen.cassandra.util.logging.Log;
import com.toddfast.mutagen.cassandra.util.logging.LogCreator;

public class ConsoleLogCreator implements LogCreator {
    private final Level level;

    /**
     * Creates a new Console Log Creator.
     *
     * @param level
     *            The minimum level to log at.
     */
    public ConsoleLogCreator(Level level) {
        this.level = level;
    }

    public Log createLogger(Class<?> clazz) {
        return new ConsoleLog(level);
    }
}
