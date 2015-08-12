package com.toddfast.mutagen.cassandra.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Collections2;
import com.toddfast.mutagen.Mutation;
import info.archinnov.achilles.junit.AchillesResource;
import info.archinnov.achilles.junit.AchillesResourceBuilder;

import java.io.IOException;
import java.util.List;

import info.archinnov.achilles.script.ScriptExecutor;
import org.junit.Before;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.toddfast.mutagen.Plan;
import com.toddfast.mutagen.cassandra.CassandraMutagen;
import com.toddfast.mutagen.cassandra.util.DBUtils;
import org.junit.Rule;

import javax.annotation.Nullable;

/**
 * 
 * Abstract test class.
 */
public abstract class AbstractTest {

    @Rule
    public AchillesResource resource = AchillesResourceBuilder
            .noEntityPackages()
            .withKeyspaceName("apispark")
            .build();

    private Session session = resource.getNativeSession();

    private ScriptExecutor scriptExecutor = resource.getScriptExecutor();

    public Plan.Result<String> result;

    /**
     * initiation for test.
     */
    @Before
    public void init() {
        // purge all table from keyspace
        DBUtils.purgeKeyspace(session);
    }
    /**
     * Get an instance of cassandra mutagen and mutate the mutations.
     * 
     * @return
     *         the result of mutations.
     * 
     */
    protected void mutate(String path) {

        // Get an instance of CassandraMutagen
        CassandraMutagen mutagen = new CassandraMutagenImpl(session);
        // Initialize the list of mutations
        try {
            mutagen.setLocation(path);
            mutagen.initialize();
            result = mutagen.mutate(false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * check if the mutations are successful.
     */
    protected void checkMutationSuccessful() {
        System.out.println("Mutation complete: " + result.isMutationComplete());
        System.out.println("Exception: " + result.getException());
        if (result.getException() != null) {
            result.getException().printStackTrace();
        }
        printMutations("Completed mutations:", result.getCompletedMutations());
        printMutations("Remaining mutations:", result.getRemainingMutations());

        // Check for completion and errors
        assertTrue(result.isMutationComplete());
        assertNull(result.getException());
    }

    protected void printMutations(String title, List<Mutation<String>> mutations) {
        System.out.println(title + Collections2.transform(mutations, new Function<Mutation<?>, String>() {
            @Nullable
            @Override
            public String apply(Mutation<?> mutation) {
                return "\n\t- " + mutation;
            }
        }));
    }

    /**
     * check the last timestamp of migration.
     * 
     * @param expectedTimestamp
     *            the expected timestamp.
     */
    protected void checkLastTimestamp(String expectedTimestamp) {
        assertEquals(expectedTimestamp, result.getLastState().getID());
    }

    // STATEMENTS

    /**
     * @param values
     *            the values to be binded for a query
     * @return
     *         the result set of a query
     */
    protected ResultSet query(Object... values) {
        String columnFamily = "Test1";
        // query
        String selectStatement = "SELECT * FROM \"" + columnFamily + "\" " + "WHERE key=?";
        PreparedStatement preparedSelectStatement = session.prepare(selectStatement);
        BoundStatement boundSelectStatement = preparedSelectStatement.bind(values);
        return session.execute(boundSelectStatement);
    }

    /**
     * Get the query result by primary key.
     * 
     * @param pk
     *            primary key.
     * @return
     *         the query result.
     */
    protected Row getByPk(String pk) {
        ResultSet results = query(pk);
        return results.one();
    }

    public String queryDatabaseForLastState() {
        ResultSet rs = getSession().execute("SELECT versionid FROM \"Version\";");

        String version = "000000000000";

        while (!rs.isExhausted()) {
            Row r = rs.one();
            String versionid = r.getString("versionid");
            if (version.compareTo(versionid) < 0)
                version = versionid;
        }

        return version;

    }

    public Plan.Result<String> getResult() {
        return result;
    }

    public ScriptExecutor getScriptExecutor() {
        return scriptExecutor;
    }

    public Session getSession() {
        return session;
    }

}
