package com.toddfast.mutagen.cassandra.impl;

import java.io.IOException;

import org.junit.Assert;

import org.junit.Test;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.toddfast.mutagen.cassandra.CassandraMutagen;

public class CleanCommandTest extends AbstractTest {
    
    @Test
    public void checkVersionTableDropped() throws IOException {
        
        // Instanciate mutagen
        CassandraMutagen mutagen = new CassandraMutagenImpl(getSession());

        mutagen.migrate("mutations/tests/execution");
        // Clean
        mutagen.clean();

        // Try select on version table. Should fail since table dropped
        try {
            getSession().execute("SELECT * FROM \"Version\";");
            Assert.fail();
        } catch (InvalidQueryException e) {
            // table has been dropped
        }

    }

}
