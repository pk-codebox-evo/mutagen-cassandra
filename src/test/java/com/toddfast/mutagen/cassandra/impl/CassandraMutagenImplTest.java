package com.toddfast.mutagen.cassandra.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import info.archinnov.achilles.junit.AchillesResource;
import info.archinnov.achilles.junit.AchillesResourceBuilder;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.junit.Test;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.toddfast.mutagen.Mutation;
import com.toddfast.mutagen.Plan;
import com.toddfast.mutagen.State;
import com.toddfast.mutagen.cassandra.AbstractCassandraMutation;
import com.toddfast.mutagen.cassandra.CassandraMutagen;

/**
 * 
 * @author Todd Fast
 */
public class CassandraMutagenImplTest {


    /**
     * Get an instance of cassandra mutagen and mutate the mutations.
     * 
     * @return
     *         the result of mutations.
     * 
     */
    private Plan.Result<String> mutate()
            throws IOException {

        // Get an instance of CassandraMutagen
        CassandraMutagen mutagen = new CassandraMutagenImpl();

        // Initialize the list of mutations
        String rootResourcePath = "com/toddfast/mutagen/cassandra/test/mutations";
        mutagen.initialize(rootResourcePath);

        // Mutate!
        Plan.Result<String> result = mutagen.mutate(session);

        return result;
    }



    /**
     * @param values
     *            the values to be binded for a query
     * @return
     *         the result set of a query
     */
    public ResultSet query(Object... values) {
        String columnFamily = "Test1";
        // query
        String selectStatement = "SELECT * FROM \"" + columnFamily + "\" " + "WHERE key=?";
        PreparedStatement preparedSelectStatement = session.prepare(selectStatement);
        BoundStatement boundSelectStatement = preparedSelectStatement.bind(values);
        return session.execute(boundSelectStatement);
    }

    @Test
    /**
     * Firstly,This test execute mutations.
     * Then it check the results of mutations.
     * It also checks for database content to verify if the mutations is well done.
     * @throws Exception
     */
    public void testData() throws Exception {

        //Execute mutations
        Plan.Result<String> result = mutate();

        // Check the results
        State<Integer> state = result.getLastState();

        System.out.println("Mutation complete: " + result.isMutationComplete());
        System.out.println("Exception: " + result.getException());
        if (result.getException() != null) {
            result.getException().printStackTrace();
        }
        System.out.println("Completed mutations: " + result.getCompletedMutations());
        System.out.println("Remining mutations: " + result.getRemainingMutations());
        System.out.println("Last state: " + (state != null ? state.getID() : "null"));

        
        // Check for completion and errors
        assertTrue(result.isMutationComplete());
        assertNull(result.getException());
        assertEquals((state != null ? state.getID() : "000000000000"), "201502011230");
        
        
        
        // Check database content
        ResultSet results1 = query("row1");
        Row row1 = results1.one();
        assertEquals("foo", row1.getString("value1"));
        assertEquals("bar", row1.getString("value2"));

        ResultSet results2 = query("row2");
        Row row2 = results2.one();
        assertEquals("chicken", row2.getString("value1"));
        assertEquals("sneeze", row2.getString("value2"));

        ResultSet results3 = query("row3");
        Row row3 = results3.one();

        assertEquals("bar", row3.getString("value1"));
        assertEquals("baz", row3.getString("value2"));
    }

    private void createVersionSchemaTable() {
        String dropStatement = "DROP TABLE \"" + versionSchemaTable + "\";";
        session.execute(dropStatement);
        String createStatement = "CREATE TABLE \"" +
                versionSchemaTable +
                "\"( versionid varchar, filename varchar,checksum varchar,"
                + "execution_date timestamp,execution_time int,"
                + "success boolean, PRIMARY KEY(versionid))";

        session.execute(createStatement);
    }

    private void appendOneVersionRecord(String version, String filename, String checksum, int execution_time,
            boolean success) {
        // insert version record
        String insertStatement = "INSERT INTO \"" + versionSchemaTable + "\" (versionid,filename,checksum,"
                + "execution_date,execution_time,success) "
                + "VALUES (?,?,?,?,?,?);";

        PreparedStatement preparedInsertStatement = session.prepare(insertStatement);
        session.execute(preparedInsertStatement.bind(version,
                filename,
                checksum,
                new Timestamp(new Date().getTime()),
                execution_time,
                success
                ));
    }

    private void dropTableTest() {
        String dropStatement = "DROP TABLE \"Test1\";";
        session.execute(dropStatement);
    }
    @Test
    /**
     * This test tests if the mutation are well done when there are already records in the table Version.
     * We insert a record with the timestamp 201502011200.
     * The script files with timestamp greater than 201502011200 should be executed.
     * The script files with timestamp not greater than 201502011200 should not be executed.
     * @throws Exception
     */
    public void testExecutionFiles() throws Exception {
        createVersionSchemaTable();
        appendOneVersionRecord("201502011200", "M201502011200_DoSomeThing_1111.cqlsh.txt", "checksum", 112, true);
        dropTableTest();
        
        List<String> mutations = new ArrayList<String>();
        // Execute mutations
        Plan.Result<String> result = mutate();
        // Check the results
        for (Mutation mutation : result.getCompletedMutations()) {
            AbstractCassandraMutation m = (AbstractCassandraMutation) mutation;
            mutations.add(m.getResultingState().getID());
        }
        assertEquals(false, mutations.contains("201502011200"));
        assertEquals(true, mutations.contains("201502011201"));
        assertEquals(true, mutations.contains("201502011225"));
        assertEquals(true, mutations.contains("201502011230"));
    }

    // //////////////////////////////////////////////////////////////////////////
    // Fields
    // //////////////////////////////////////////////////////////////////////////

    private static String keyspace = "apispark";

    private String versionSchemaTable = "Version";

    /**
     * Using the achilles to create session for test.
     * 
     */
    public AchillesResource resource = AchillesResourceBuilder
            .noEntityPackages().withKeyspaceName(keyspace).build();

    public Session session = resource.getNativeSession();
}
