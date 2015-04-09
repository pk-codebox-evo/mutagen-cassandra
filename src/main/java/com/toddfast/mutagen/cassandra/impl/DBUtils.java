package com.toddfast.mutagen.cassandra.impl;

import java.sql.Timestamp;
import java.util.Date;
import java.util.List;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.toddfast.mutagen.MutagenException;
import com.toddfast.mutagen.State;
import com.toddfast.mutagen.basic.SimpleState;

public class DBUtils {
    private DBUtils() {
    }

    public static void purgeKeyspace(Session session) {

        // get current keyspace
        String keyspace = session.getLoggedKeyspace();

        // list all tables of keyspace (equivalent to cqlsh DESCRIBE TABLES)
        ResultSet rs = session
                .execute("SELECT columnfamily_name FROM system.schema_columnfamilies where keyspace_name = '"
                        + keyspace + "' ;");

        // drop all tables
        while (!rs.isExhausted()) {
            Row r = rs.one();
            String tablename = r.getString("columnfamily_name");
            session.execute("DROP TABLE \"" + tablename + "\";");
        }
    }
    /**
     * Create table Version.
     * 
     */
    public static void createSchemaVersionTable(Session session) {
        // Create table if it doesn't exist
        String createStatement = "CREATE TABLE IF NOT EXISTS \"" +
                "Version" +
                "\"( versionid varchar, filename varchar,checksum varchar,"
                + "execution_date timestamp,execution_time int,"
                + "success boolean, PRIMARY KEY(versionid))";

        session.execute(createStatement);
    }

    /**
     * Execute query in the table Version.
     * 
     * @return
     *         all the records in version table.
     */

    public static ResultSet getVersionRecord(Session session) {
        // get version record
        String selectStatement = "SELECT * FROM \"" +
                "Version" + "\"" +
                "limit 1000000000;";
        return session.execute(selectStatement);
    }

    public static boolean isEmptyVersionTable(Session session) {
        ResultSet rs = session.execute("SELECT * FROM \"Version\";");
        if (rs.isExhausted())
            return true;
        return false;
    }
    
    /**
     * append the version record in the table Version.
     * 
     * @param session
     *            the session to execute cql.
     * @param version
     *            Id of version record,usually represented by the datetime.
     * @param filename
     *            name of script file that was executed.
     * @param checksum
     *            checksum for validation.
     * @param execution_time
     *            The execution time(ms) for this script file.
     * @param success
     *            represents if this execution successes.
     */
    public static void appendVersionRecord(Session session, String version, String filename, String checksum,
            int execution_time,
            boolean success) {
        // insert statement for version record
        String insertStatement = "INSERT INTO \"" + "Version" + "\" (versionid,filename,checksum,"
                + "execution_date,execution_time,success) "
                + "VALUES (?,?,?,?,?,?);";
        // prepare statement
        PreparedStatement preparedInsertStatement = session.prepare(insertStatement);
        session.execute(preparedInsertStatement.bind(version,
                filename,
                checksum,
                new Timestamp(new Date().getTime()),
                execution_time,
                success
                ));
    }

    /**
     * Find record for a given versionId
     * 
     * @param versionId
     * @return Result set with one row if versionId present, empty otherwise
     */
    public static ResultSet getVersionRecordByVersionId(Session session, String versionId) {
        String selectStatement = "SELECT * FROM \""
                + "Version"
                + "\" WHERE versionid = '" + versionId + "'";
        return session.execute(selectStatement);
    }

    /**
     * Check if the versionId exists in the database.
     * 
     * @param versionId
     * @return true if versionId is in the database
     */
    public static boolean isVersionIdPresent(Session session, String versionId) {
        return !getVersionRecordByVersionId(session, versionId).isExhausted();
    }

    /**
     * Check if the mutation fails.
     * 
     * @param versionId
     * @return true if versionId is in the database
     */
    public static boolean isMutationFailed(Session session, String versionId) {
        // get rows for given version id
        List<Row> rows = getVersionRecordByVersionId(session, versionId).all();

        // if there's one and only one row, and the mutation has failed
        if (rows.size() == 1 && !rows.get(0).getBool("success"))
            return true;
        return false;
    }

    public static boolean isMutationHashCorrect(Session session, String versionId, String hash) {
        String selectStatement = "SELECT checksum FROM \"" +
                "Version" + "\" WHERE versionid = '" + versionId + "'";
        ResultSet result = session.execute(selectStatement);

        String checksum = result.all().get(0).getString("checksum");
        return (checksum.compareTo(hash) == 0);
    }

    /**
     * Get the current timestamp in the database.
     * 
     * @return
     *         the current timestamp in the database.
     */
    public static State<String> getCurrentState(Session session) {
        String version = "000000000000";
        ResultSet results = null;
        try {
            results = getVersionRecord(session);
        } catch (Exception e) {
            try {
                createSchemaVersionTable(session);
            } catch (Exception e2) {
                throw new MutagenException("Could not create version table", e2);
            }
        }
        try {
            results = getVersionRecord(session);
        } catch (Exception e) {
            throw new MutagenException(
                    "could not retreive Version table information", e);

        }

        while (!results.isExhausted()) {
            Row r = results.one();
            String versionid = r.getString("versionid");
            if (r.getBool("success") == true && version.compareTo(versionid) < 0)
                version = versionid;
        }

        return new SimpleState<String>(version);
    }
}