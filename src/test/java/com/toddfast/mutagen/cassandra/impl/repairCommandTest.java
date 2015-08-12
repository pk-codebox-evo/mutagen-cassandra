package com.toddfast.mutagen.cassandra.impl;

import java.io.IOException;

import org.junit.Assert;

import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.toddfast.mutagen.cassandra.CassandraMutagen;

public class repairCommandTest extends AbstractTest {

    @Test
    public void testFailuresRemoved() throws IOException {

        String resourcePath = "mutations/tests/failed_mutation";

        // mutate with failure
        mutate(resourcePath);
        Assert.assertNotNull(getResult().getException());

        // Make sure there was a recorded failure
        ResultSet rs = getSession().execute("SELECT status FROM \"Version\"");

        boolean mutationHasFailed = false;
        while (!rs.isExhausted()) {
            if (rs.one().getString(0).equals("Failed")) {
                mutationHasFailed = true;
            }
        }
        Assert.assertTrue(mutationHasFailed);


        // Instanciate new mutagen object
        CassandraMutagen mutagen = new CassandraMutagenImpl(getSession());
        mutagen.setLocation(resourcePath);
        mutagen.initialize();

        // Repair
        mutagen.repair();

        // make sure there's no failure in the database
        rs = getSession().execute("SELECT status FROM \"Version\"");

        while (!rs.isExhausted()) {
            if (rs.one().getString(0).equals("Failed")) {
                Assert.fail();
            }
        }

    }

}
