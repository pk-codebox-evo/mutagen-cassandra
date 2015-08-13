package com.toddfast.mutagen.cassandra.impl;

import com.toddfast.mutagen.cassandra.MutationStatus;
import org.junit.Test;

import com.toddfast.mutagen.MutagenException;
import com.toddfast.mutagen.cassandra.util.DBUtils;


public class UnexpectedNewMigrationTest extends AbstractTest {

    /**
     * Check that creating a new migration with older state throws exception.
     */
    @Test(expected = MutagenException.class)
    public void testAddedScriptWithInferiorState() {

        DBUtils.createSchemaVersionTable(getSession());

        // mutation with versionId ending with 2
        DBUtils.appendVersionRecord(getSession(), "201501010002", "Foo", "", 0, MutationStatus.SUCCESS.getValue());

        // try to mutate with script ending in 1
        mutate("mutations/tests/unexpected_new_migration");

    }

}
