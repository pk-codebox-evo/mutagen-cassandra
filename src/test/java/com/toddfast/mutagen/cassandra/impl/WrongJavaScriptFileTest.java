package com.toddfast.mutagen.cassandra.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import com.toddfast.mutagen.MutagenException;

public class WrongJavaScriptFileTest extends AbstractTest {
    @Test
    public void migration_with_wrong_java_cql_statements() {

        // Execute mutations
        mutate("mutations/tests/wrongJavaScript");

        // wrong java script file throws mutagen exception
        assertNotNull(result.getException());

    }
}
