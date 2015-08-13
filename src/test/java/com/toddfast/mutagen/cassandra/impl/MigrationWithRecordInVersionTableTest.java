package com.toddfast.mutagen.cassandra.impl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import com.toddfast.mutagen.cassandra.MutationStatus;
import org.junit.Test;

import com.toddfast.mutagen.Mutation;
import com.toddfast.mutagen.cassandra.utils.DBUtils;


public class MigrationWithRecordInVersionTableTest extends AbstractTest {


     /**
     * This test tests if the mutation are well done when there are already records in the table Version.
     * We insert a record with the timestamp 201502011209.
     * The script files with timestamp greater than 201502011209 should be executed.
     * The script files with timestamp not greater than 201502011209 should not be executed.
     * 
     */
    @Test
    public void migration_with_record_version_table() {

        DBUtils.createSchemaVersionTable(getSession());
        // append two version record
        DBUtils.appendVersionRecord(getSession(), "201502011200", "M201502011200_DoSomeThing_1111.cqlsh.txt",
                "5ac70f706156a3264c518f0c7d754f7f", 112, MutationStatus.SUCCESS.getValue());
        DBUtils.appendVersionRecord(getSession(), "201502011209", "M201502011209_DoSomeThing_1111.cqlsh.txt", "",
                112, MutationStatus.SUCCESS.getValue());
        // Execute mutations
        mutate("mutations/tests/execution");

        // Get the mutations executed
        List<String> mutations = new ArrayList<>();
        for (Mutation<String> mutation : result.getCompletedMutations())
            mutations.add(mutation.getResultingState().getID());

        assertFalse(mutations.contains("201502011200"));
        assertTrue(mutations.contains("201502011210"));
     }



}
