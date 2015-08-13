package com.toddfast.mutagen.cassandra.utils;

import ch.qos.logback.classic.Level;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.toddfast.mutagen.Mutation;
import com.toddfast.mutagen.Plan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author Manuel Boillod
 */
public class MutagenUtils {

    private static Logger LOGGER = LoggerFactory.getLogger(MutagenUtils.class);

    /**
     * Show the result of migrations.
     *
     * @param result
     *            - the result of migration.
     */
    public static void showMigrationResults(Plan.Result<String> result) {
        printMutations("Completed mutations:", result.getCompletedMutations());
        printMutations("Remaining mutations:", result.getRemainingMutations());
        if (result.isMutationComplete()) {
            LOGGER.info("Migration finished.");
        } else {
            LOGGER.error("Migration aborted!");
        }
    }

    public static void printMutations(String title, List<Mutation<String>> mutations) {
        LOGGER.info(title + Collections2.transform(mutations, new Function<Mutation<?>, String>() {
            @Override
            public String apply(Mutation<?> mutation) {
                return "\n\t- " + mutation;
            }
        }));
    }

    public static void initLogging(Level level) {
        ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) LoggerFactory
                .getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);

        root.setLevel(level);
    }
}
