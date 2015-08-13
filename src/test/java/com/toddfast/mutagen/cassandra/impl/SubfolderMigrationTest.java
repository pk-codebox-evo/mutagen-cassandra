package com.toddfast.mutagen.cassandra.impl;

import com.datastax.driver.core.Row;
import com.toddfast.mutagen.MutagenException;
import com.toddfast.mutagen.cassandra.MutationStatus;
import com.toddfast.mutagen.cassandra.impl.info.MigrationInfo;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SubFolderMigrationTest extends AbstractTest {

    @Test
    public void execute_folder_v1_then_v2() {

        // Execute mutations
        mutate("mutations/tests/subfolder/v1");

        // Check the results
        checkMutationSuccessful();

        // Execute mutations
        mutate("mutations/tests/subfolder/v2");

        // Check the results
        checkMutationSuccessful();

        MigrationInfo[] migrationInfo = getMigrationInfo();
        Assert.assertEquals(2, migrationInfo.length);
        Assert.assertEquals(MutationStatus.SUCCESS.getValue(), migrationInfo[0].getStatus());
        Assert.assertEquals(MutationStatus.SUCCESS.getValue(), migrationInfo[1].getStatus());

    }

    @Test
    public void execute_folder_v2_then_v1() {

        // Execute mutations
        mutate("mutations/tests/subfolder/v2");

        // Check the results
        checkMutationSuccessful();

        MigrationInfo[] migrationInfo = getMigrationInfo();
        Assert.assertEquals(1, migrationInfo.length);
        Assert.assertEquals(MutationStatus.SUCCESS.getValue(), migrationInfo[0].getStatus());


        // Execute mutations fails because script is anterior
        try {
            mutate("mutations/tests/subfolder/v1");
            Assert.fail("should fail");
        } catch (MutagenException e) {
            Assert.assertEquals("Mutation has state (state=201501010001) inferior to current state (state=201501010002) but was not recorded in the database",
                    e.getMessage());
        }

        // Check the mutation has not been executed
        checkMutationSuccessful();

        migrationInfo = getMigrationInfo();
        Assert.assertEquals(1, migrationInfo.length);
        Assert.assertEquals(MutationStatus.SUCCESS.getValue(), migrationInfo[0].getStatus());

    }

}
