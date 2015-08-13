package com.toddfast.mutagen.cassandra.impl;

import com.toddfast.mutagen.MutagenException;
import com.toddfast.mutagen.cassandra.MutationStatus;
import com.toddfast.mutagen.cassandra.impl.info.MigrationInfo;
import org.junit.Assert;
import org.junit.Test;

public class FailedCqlMutationTest extends AbstractTest {

    /*
     * Test for error flag from previous execution in database
     * There are 3 scripts, the first and last are successful, the second one fails
     */
    @Test
    public void failedMutationShouldThrowError() {
        firstMutationFailed();
        secondMutationAbortedBecauseMutationStateIsFailed();
    }

    private void firstMutationFailed() {
        mutate("mutations/tests/failed_cql_mutation");
        Assert.assertNotNull(getResult().getException());

        MigrationInfo[] migrationInfo = getMigrationInfo();
        Assert.assertEquals(2, migrationInfo.length);
        Assert.assertEquals(MutationStatus.SUCCESS.getValue(), migrationInfo[0].getStatus());
        Assert.assertEquals(MutationStatus.FAILED.getValue(), migrationInfo[1].getStatus());
    }

    private void secondMutationAbortedBecauseMutationStateIsFailed() {
        // migrate when database is in failure will failed with a
        try {
            mutate("mutations/tests/failed_cql_mutation");
            Assert.fail("should fail with a MutagenException");
        } catch (MutagenException e) {
            Assert.assertEquals("There is a failed mutation in database for script : M201501010002_WrongCqlScriptFile_1111.cqlsh.txt[state=201501010002]",
                    e.getMessage());
        }

        MigrationInfo[] migrationInfo = getMigrationInfo();
        Assert.assertEquals(2, migrationInfo.length);
        Assert.assertEquals(MutationStatus.SUCCESS.getValue(), migrationInfo[0].getStatus());
        Assert.assertEquals(MutationStatus.FAILED.getValue(), migrationInfo[1].getStatus());
    }
}
