package com.toddfast.mutagen.cassandra.impl;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;

public class BaselineCommandTest extends AbstractTest {

    final String desiredLastState = "201502011225";

    final String resourcePath = "mutations/tests/baseline";

    @Test
    public void checkForLastStateAndChecksums() throws IOException {

        // Instanciate mutagen
        CassandraMutagenImpl mutagen = new CassandraMutagenImpl(getSession());
        mutagen.setBaselineVersion(desiredLastState);
        mutagen.initialize(resourcePath);

        // set baseline for third of four scripts
        // first three scripts contains error, to check for unexpected execution
        mutagen.baseline();
        
        Assert.assertEquals(desiredLastState, queryDatabaseForLastState());
        
        // mutate to check for checksum errors and failure to restart
        mutate(resourcePath);

        // verify that no exception occurred
        Assert.assertNull(getResult().getException());
        
        // verify that last state matches last script
        checkLastTimestamp("201502011230");

    }
}
