package com.toddfast.mutagen.cassandra.impl;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

public class WrongCqlScriptFileTest extends AbstractTest {

    @Test
    public void migration_with_wrong_cql_statements() {

        // Execute mutations
        mutate("mutations/tests/wrongCqlScript");

        // wrong cql script file throws mutagen exception
        assertNotNull(result.getException());
    }
}
