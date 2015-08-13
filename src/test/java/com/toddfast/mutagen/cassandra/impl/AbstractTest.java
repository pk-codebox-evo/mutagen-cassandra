package com.toddfast.mutagen.cassandra.impl;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.toddfast.mutagen.Plan;
import com.toddfast.mutagen.cassandra.CassandraMutagen;
import com.toddfast.mutagen.cassandra.impl.info.MigrationInfo;
import com.toddfast.mutagen.cassandra.impl.info.MigrationInfoCommand;
import com.toddfast.mutagen.cassandra.impl.info.MigrationInfoService;
import com.toddfast.mutagen.cassandra.utils.DBUtils;
import com.toddfast.mutagen.cassandra.utils.MutagenUtils;
import info.archinnov.achilles.junit.AchillesResource;
import info.archinnov.achilles.junit.AchillesResourceBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public abstract class AbstractTest {

    protected Logger LOGGER = LoggerFactory.getLogger(getClass());

    @Rule
    public AchillesResource resource = AchillesResourceBuilder
            .noEntityPackages()
            .withKeyspaceName("apispark")
            .build();

    private Session session = resource.getNativeSession();

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
            printMigrationInfo();
        }
    }

    /**
     * Print the mutations table
     */
    protected void printMigrationInfo() {
        LOGGER.info("\n" + MigrationInfoCommand.printInfo(getMigrationInfo()));
    }

    /**
     * check if the mutations are successful.
     */
    protected void checkMutationSuccessful() {
        MutagenUtils.showMigrationResults(result);

        // Check for completion and errors
        assertTrue(result.isMutationComplete());
        assertNull(result.getException());
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

    public Plan.Result<String> getResult() {
        return result;
    }

    public Session getSession() {
        return session;
    }

}
