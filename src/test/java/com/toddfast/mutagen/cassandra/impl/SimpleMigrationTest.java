package com.toddfast.mutagen.cassandra.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.toddfast.mutagen.cassandra.MutationStatus;
import com.toddfast.mutagen.cassandra.impl.info.MigrationInfo;
import org.junit.Assert;
import org.junit.Test;

import com.datastax.driver.core.Row;

public class SimpleMigrationTest extends AbstractTest {
    /**
     * Firstly,This test execute mutations.
     * Then it check the results of mutations.
     * It also checks for database content to verify if the mutations is well done.
     * 
     */
    @Test
    public void simple_cql_migration() {

        // Execute mutations
        mutate("mutations/tests/simple/cql");

        // Check the results
        checkMutationSuccessful();

        // Check the last timestamp
        checkLastTimestamp("201502011200");

        // Check database content
        Row row1 = getByPk("row1");
        assertNotNull(row1);
        assertEquals("value1", row1.getString("value1"));

        MigrationInfo[] migrationInfo = getMigrationInfo();
        Assert.assertEquals(1, migrationInfo.length);
        Assert.assertEquals("201502011200", migrationInfo[0].getVersion());
        Assert.assertEquals("M201502011200_CreateTableTest_1000.cqlsh.txt", migrationInfo[0].getFilename());
        Assert.assertEquals(MutationStatus.SUCCESS.getValue(), migrationInfo[0].getStatus());

    }

    @Test
    public void simple_java_migration() {

        // Execute mutations
        mutate("mutations/tests/simple/java");

        // Check the results
        checkMutationSuccessful();

        // Check the last timestamp
        checkLastTimestamp("201502011200");

        // Check database content
        Row row1 = getByPk("row1");
        assertNotNull(row1);
        assertEquals("value1", row1.getString("value1"));

        MigrationInfo[] migrationInfo = getMigrationInfo();
        Assert.assertEquals(1, migrationInfo.length);
        Assert.assertEquals("201502011200", migrationInfo[0].getVersion());
        Assert.assertEquals("M201502011200_CreateTableTest_1000.java", migrationInfo[0].getFilename());
        Assert.assertEquals(MutationStatus.SUCCESS.getValue(), migrationInfo[0].getStatus());
    }
}
