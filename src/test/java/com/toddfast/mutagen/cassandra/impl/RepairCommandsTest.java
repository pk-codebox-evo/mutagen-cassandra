package com.toddfast.mutagen.cassandra.impl;

import java.io.IOException;

import com.toddfast.mutagen.cassandra.MutationStatus;
import com.toddfast.mutagen.cassandra.impl.info.MigrationInfo;
import org.junit.Assert;

import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.toddfast.mutagen.cassandra.CassandraMutagen;

public class RepairCommandsTest extends AbstractTest {

    @Test
    public void testFailuresRemoved() throws IOException {

        String resourcePath = "mutations/tests/repair";

        // mutate with failure
        mutate(resourcePath);
        Assert.assertNotNull(getResult().getException());

        MigrationInfo[] migrationInfo = getMigrationInfo();
        Assert.assertEquals(2, migrationInfo.length);
        Assert.assertEquals(MutationStatus.SUCCESS.getValue(), migrationInfo[0].getStatus());
        Assert.assertEquals(MutationStatus.FAILED.getValue(), migrationInfo[1].getStatus());


        // Instanciate new mutagen object
        CassandraMutagen mutagen = new CassandraMutagenImpl(getSession());
        mutagen.setLocation(resourcePath);
        mutagen.initialize();

        // Repair
        mutagen.repair();


        migrationInfo = getMigrationInfo();
        Assert.assertEquals(1, migrationInfo.length);
        for (MigrationInfo migration : migrationInfo) {
            Assert.assertEquals(MutationStatus.SUCCESS.getValue(), migration.getStatus());
        }
    }

}
