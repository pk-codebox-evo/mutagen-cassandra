package com.toddfast.mutagen.cassandra.impl;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.exceptions.QueryValidationException;
import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import com.toddfast.mutagen.MutagenException;
import com.toddfast.mutagen.cassandra.AbstractCassandraMutation;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Generate the mutation for the script file end with cqlsh.txt.
 */
public class CqlMutation extends AbstractCassandraMutation {

    private String resourceName;

    /**
     * constructor for CQLMutation.
     *
     * @param session      the session to execute cql statements.
     * @param resourceName name of script file end with cqlsh.txt.
     */
    public CqlMutation(Session session, String resourceName) {
        super(session);
        this.resourceName = resourceName;
    }

    /**
     * Return md5 hash of source
     */
    @Override
    public String getChecksum() {
        try (InputStream resourceInputStream = getResourceInputStream()) {
            return toHex(md5(resourceInputStream));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the ressource name.
     */
    @Override
    public String getResourceName() {
        return Paths.get(resourceName).getFileName().toString();
    }

    /**
     * Divide the script file end with .cqlsh.txt into statements.
     */
    private List<String> getCQLStatements() {
        List<String> statements = new ArrayList<>();

        try (InputStream resourceInputStream = getResourceInputStream()) {
            List<String> lines = CharStreams.readLines(new InputStreamReader(resourceInputStream, Charsets.UTF_8));
            StringBuilder statement = new StringBuilder();
            for (String line : lines) {
                int index;
                String trimmedLine = line.trim();

                if (trimmedLine.startsWith("--") || trimmedLine.startsWith("//")) {
                    // Skip
                    continue;
                }

                if ((index = line.lastIndexOf(";")) != -1) {
                    // Split the line at the semicolon
                    statement
                            .append("\n")
                            .append(line.substring(0, index + 1));
                    statements.add(statement.toString());

                    if (line.length() > index + 1) {
                        statement = new StringBuilder(line.substring(index + 1));
                    } else {
                        statement = new StringBuilder();
                    }
                } else {
                    statement
                            .append("\n")
                            .append(line);
                }

            }
            return statements;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * load resource by path.
     *
     * @return the content of resource
     */
    private InputStream getResourceInputStream() {

        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        if (loader == null) {
            loader = getClass().getClassLoader();
        }

        InputStream inputStream = loader.getResourceAsStream(resourceName);
        if (inputStream == null) {
            File file = new File(resourceName);
            if (file.exists()) {
                try {
                    inputStream = new FileInputStream(file);
                } catch (FileNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        if (inputStream == null) {
            throw new IllegalArgumentException("Resource \"" + resourceName + "\" not found");
        }

        return inputStream;
    }

    /**
     * Performs the mutation using the cassandra context.
     */
    @Override
    protected void performMutation(Context context) {
        context.info("Executing mutation {}", getResultingState().getID());

        List<String> statements = getCQLStatements();

        for (String statement : statements) {
            context.debug("Executing CQL statement \"{}\"", statement);
            try {
                // execute the cql statement
                ResultSet result = getSession().execute(statement);
                context.debug("Successfully executed CQL statement \"{}\" in {} attempts",
                        statement, result);
            } catch (QueryValidationException e) {
                context.error("Statement Validatation Exception executing CQL \"{}\"", statement, e);
                throw new MutagenException("Statement Validation Exception executing CQL \"" +
                        statement + "\"", e);
            } catch (QueryExecutionException e) {
                context.error("Statement Execution Exception executing CQL \"{}\"", statement, e);
                throw new MutagenException("Statement Execution Exception executing CQL \"" +
                        statement + "\"", e);
            }
        }
        context.info("Done executing mutation {}", getResultingState().getID());
    }

}
