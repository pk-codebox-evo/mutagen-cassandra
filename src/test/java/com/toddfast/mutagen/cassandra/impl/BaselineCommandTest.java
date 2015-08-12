package com.toddfast.mutagen.cassandra.impl;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import com.toddfast.mutagen.cassandra.CassandraMutagen;
import com.toddfast.mutagen.cassandra.util.DBUtils;

public class BaselineCommandTest extends AbstractTest {

    final String desiredLastState = "201502011224";

    final String resourcePath = "mutations/tests/baseline";

    @Test
    public void checkForLastStateAndChecksums() throws IOException {

        // Instanciate mutagen
        CassandraMutagen mutagen = new CassandraMutagenImpl(getSession());
        mutagen.setBaselineVersion(desiredLastState);
        mutagen.setLocation(resourcePath);
        mutagen.initialize();

        // set baseline for third of four scripts
        // first three scripts contains error, to check for unexpected execution
        mutagen.baseline();
        Assert.assertEquals(desiredLastState, DBUtils.getCurrentState(getSession()));
        // mutate to check for checksum errors and failure to restart
        mutate(resourcePath);

        // verify that no exception occurred
        Assert.assertNotNull(getResult().getException());
        
        Assert.assertEquals(desiredLastState, DBUtils.getCurrentState(getSession()));

    }
}
