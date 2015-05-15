package com.toddfast.mutagen.cassandra.impl;

import org.junit.Assert;
import org.junit.Test;

import com.toddfast.mutagen.cassandra.CassandraMutagen;
import com.toddfast.mutagen.cassandra.impl.info.MigrationInfoService;

public class MigrationInfoTest extends AbstractTest {
    @Test
    public void noMigrationInfoTest() {
        CassandraMutagen mutagen = new CassandraMutagenImpl(getSession());
        MigrationInfoService migrationService = mutagen.info();
        migrationService.refresh();
        Assert.assertEquals("no migrations found", migrationService.toString());
    }

    @Test
    public void migrationInfoWithFailedScriptTest() {
        CassandraMutagenImpl mutagen = new CassandraMutagenImpl(getSession());
        MigrationInfoService migrationService = mutagen.info();

        mutagen.migrate("mutations/tests/failed_mutation");

        migrationService.refresh();
        Assert.assertEquals(migrationService.current().getFilename(), "M201507010001_WrongCqlScriptFile_1111.cqlsh.txt");
    }

    @Test
    public void migrationInfoWithNoFailedScriptTest() {
        CassandraMutagen mutagen = new CassandraMutagenImpl(getSession());
        MigrationInfoService migrationService = mutagen.info();

        mutagen.migrate("mutations/tests/execution");

        migrationService.refresh();

        Assert.assertEquals(migrationService.failed(), null);
        Assert.assertEquals(migrationService.success().length, 4);
        Assert.assertEquals(migrationService.all().length, 4);
        Assert.assertEquals(migrationService.current().getFilename(), "M201502011230_AddTableTest_1111.cqlsh.txt");
        Assert.assertEquals(migrationService.current().getStatus(), "Success");
    }
}
