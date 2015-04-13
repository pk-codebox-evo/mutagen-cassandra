package com.toddfast.mutagen.cassandra.util.logging.slf4j;

import org.slf4j.LoggerFactory;

import com.toddfast.mutagen.cassandra.util.logging.Log;
import com.toddfast.mutagen.cassandra.util.logging.LogCreator;

public class Slf4jLogCreator implements LogCreator {
    public Log createLogger(Class<?> clazz) {
        return new Slf4jLog(LoggerFactory.getLogger(clazz));
    }
}
