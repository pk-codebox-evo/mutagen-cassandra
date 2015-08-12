package com.toddfast.mutagen.cassandra.impl;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import ch.qos.logback.classic.Level;
import com.toddfast.mutagen.cassandra.commandline.Main;
import org.junit.Test;

import com.toddfast.mutagen.Mutation;

public class MigrationOrderTest extends AbstractTest {
    /**
     * This test tests if the order of execution is according to the timestamp of script file.
     */
    @Test
    public void migration_execution_order() {
        // Execute mutations
        mutate("mutations/tests/execution");

        checkMutationSuccessful();

        // Get the mutations executed
        assertEquals(4, result.getCompletedMutations().size());

        List<String> mutations = new ArrayList<>();
        for (Mutation<String> mutation : result.getCompletedMutations()) {
            mutations.add(mutation.getResultingState().getID());
        }

        assertEquals("201502011200", mutations.get(0));
        assertEquals("201502011210", mutations.get(1));
        assertEquals("201502011225", mutations.get(2));
        assertEquals("201502011230", mutations.get(3));
    }
}
