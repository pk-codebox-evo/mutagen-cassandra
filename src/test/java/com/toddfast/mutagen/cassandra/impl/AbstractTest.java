package com.toddfast.mutagen.cassandra.impl;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.toddfast.mutagen.Mutation;
import com.toddfast.mutagen.Plan;
import com.toddfast.mutagen.cassandra.CassandraMutagen;
import com.toddfast.mutagen.cassandra.impl.info.MigrationInfo;
import com.toddfast.mutagen.cassandra.impl.info.MigrationInfoCommand;
import com.toddfast.mutagen.cassandra.impl.info.MigrationInfoService;
import com.toddfast.mutagen.cassandra.util.DBUtils;
import info.archinnov.achilles.junit.AchillesResource;
import info.archinnov.achilles.junit.AchillesResourceBuilder;
import info.archinnov.achilles.script.ScriptExecutor;
import org.junit.Before;
import org.junit.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * 
 * Abstract test class.
 */
public abstract class AbstractTest {

    protected Logger LOGGER = LoggerFactory.getLogger(getClass());

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
        } finally {
            printMutationInfo();
        }
    }

    /**
     * Print the mutations table
     */
    protected void printMutationInfo() {
        LOGGER.info("\n" + MigrationInfoCommand.printInfo(getMigrationInfo()));
    }

    /**
     * check if the mutations are successful.
     */
    protected void checkMutationSuccessful() {
        LOGGER.info("Mutation complete: " + result.isMutationComplete());
        LOGGER.info("Exception: ", result.getException());
        printMutations("Completed mutations:", result.getCompletedMutations());
        printMutations("Remaining mutations:", result.getRemainingMutations());

        // Check for completion and errors
        assertTrue(result.isMutationComplete());
        assertNull(result.getException());
    }

    protected void printMutations(String title, List<Mutation<String>> mutations) {
        LOGGER.info(title + Collections2.transform(mutations, new Function<Mutation<?>, String>() {
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
     * Print the mutations table
     */
    protected void printMigrationInfo() {
        LOGGER.info("Migration info:\n" + MigrationInfoCommand.printInfo(getMigrationInfo()));
    }

    protected MigrationInfo[] getMigrationInfo() {
        MigrationInfoService migrationInfoService = getMigrationInfoService();
        migrationInfoService.refresh();
        return migrationInfoService.all();
    }

    protected MigrationInfoService getMigrationInfoService() {
        // Get an instance of CassandraMutagen
        CassandraMutagen mutagen = new CassandraMutagenImpl(session);
        return mutagen.info();
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
