package com.toddfast.mutagen.cassandra.impl;

import com.toddfast.mutagen.cassandra.MutationStatus;
import org.junit.Assert;
import org.junit.Test;

import com.toddfast.mutagen.cassandra.impl.info.MigrationInfoService;

public class MigrationInfoTest extends AbstractTest {
    @Test
    public void noMigrationInfoTest() {
        MigrationInfoService migrationService = getMigrationInfoService();
        migrationService.refresh();
        Assert.assertEquals("no migrations found", migrationService.toString());
    }

    @Test
    public void migrationInfoWithFailedScriptTest() {
        mutate("mutations/tests/failed_cql_mutation");

        MigrationInfoService migrationInfoService = getMigrationInfoService();
        migrationInfoService.refresh();
        Assert.assertEquals("M201501010002_WrongCqlScriptFile_1111.cqlsh.txt", migrationInfoService.current().getFilename());
    }

    @Test
    public void migrationInfoWithNoFailedScriptTest() {

        mutate("mutations/tests/execution");

        MigrationInfoService migrationService = getMigrationInfoService();
        migrationService.refresh();
        Assert.assertNull(migrationService.failed());
        Assert.assertEquals(4, migrationService.success().length);
        Assert.assertEquals(4, migrationService.all().length);
        Assert.assertEquals("M201502011230_AddTableTest_1111.cqlsh.txt", migrationService.current().getFilename());
        Assert.assertEquals(MutationStatus.SUCCESS.getValue(), migrationService.current().getStatus());
    }
}
