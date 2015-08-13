package com.toddfast.mutagen.cassandra.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.toddfast.mutagen.MutagenException;

public class WrongCqlScriptFileTest extends AbstractTest {

    @Test
    public void migration_with_wrong_cql_statements() {

        // Execute mutations
        mutate("mutations/tests/wrongCqlScript");

        // wrong cql script file throws mutagen exception
        assertNotNull(result.getException());
    }
}
