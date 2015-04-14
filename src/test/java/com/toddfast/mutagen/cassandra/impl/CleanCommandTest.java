package com.toddfast.mutagen.cassandra.impl;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;

import com.datastax.driver.core.exceptions.InvalidQueryException;

public class CleanCommandTest extends AbstractTest {
    
    @Test
    public void checkVersionTableDropped() throws IOException {
        
        // Instanciate mutagen
        CassandraMutagenImpl mutagen = new CassandraMutagenImpl(getSession());

        // Use working case for test
        mutagen.setLocation("mutations/tests/execution");
        mutagen.initialize();

        // mutate
        mutagen.mutate();

        // Clean
        mutagen.clean();

        // Try select on version table. Should fail since table dropped
        try {
            getSession().execute("SELECT * FROM \"Version\";");
            Assert.fail();
        } catch (InvalidQueryException e) {
            // table has been dropped
        };

    }

}
