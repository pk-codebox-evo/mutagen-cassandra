package com.toddfast.mutagen.cassandra.util;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.toddfast.mutagen.cassandra.MutationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.toddfast.mutagen.MutagenException;

public class DBUtils {
    private static Logger log = LoggerFactory.getLogger(DBUtils.class);

    private DBUtils() {
    }

    public static void purgeKeyspace(Session session) {
        log.trace("Entering purgeKeyspace(session={})", session);

        // get current keyspace
        String keyspace = session.getLoggedKeyspace();

        // list all tables of keyspace (equivalent to cqlsh DESCRIBE TABLES)
        log.trace("Listing all tables for keyspace {}", keyspace);
        ResultSet rs = session
                .execute("SELECT columnfamily_name FROM system.schema_columnfamilies where keyspace_name = '"
                        + keyspace + "' ;");

        // drop all tables
        while (!rs.isExhausted()) {
            Row r = rs.one();
            String tablename = r.getString("columnfamily_name");

            log.trace("Dropping table {} of keyspace {}", tablename, keyspace);
            session.execute("DROP TABLE \"" + tablename + "\";");
        }
        log.trace("Leaving purgeKeyspace()");

    }

    /**
     * Create table Version.
     * 
     */
    public static void createSchemaVersionTable(Session session) {
        log.trace("Entering createSchemaVersionTable(session={})", session);

        // Create table if it doesn't exist
        String createStatement = "CREATE TABLE IF NOT EXISTS \"" +
                "Version" +
                "\"( versionid varchar, filename varchar,checksum varchar,"
                + "execution_date timestamp,execution_time int,"
                + "status varchar, PRIMARY KEY(versionid))";

        session.execute(createStatement);
        log.trace("Leaving createSchemaVersionTable()");

    }

    /**
     * Drop table Version.
     */
    public static void dropSchemaVersionTable(Session session) {
        log.trace("Dropping version table");
        String dropStatement = "DROP TABLE IF EXISTS \"Version\";";
        session.execute(dropStatement);
    }

    /**
     * Execute query in the table Version.
     * 
     * @return
     *         all the records in version table.
     */

    public static ResultSet getVersionRecords(Session session) {
        log.trace("getting version records");

        // get version record
        String selectStatement = "SELECT * FROM \"" +
                "Version" + "\"" +
                "limit 1000000000;";
        return session.execute(selectStatement);
    }

    /**
     * check if the table is empty.
     * 
     * @param session
     *            - the session to execute cql.
     * @return - bool
     */
    public static boolean isEmptyVersionTable(Session session) {
        log.trace("Checking for empty version table");

        ResultSet rs = session.execute("SELECT * FROM \"Version\";");
        return rs.isExhausted();
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
     * @param status
     *            represents the result of migration.
     */
    public static void appendVersionRecord(Session session, String version, String filename, String checksum,
            int execution_time,
            String status) {

        log.trace(
                "Entering appendVersionRecord(session={}, version={}, filename={}, checksum={}, execution_time={}, status={})",
                session, version, filename, checksum, execution_time, status);

        // insert statement for version record
        String insertStatement = "INSERT INTO \"" + "Version" + "\" (versionid,filename,checksum,"
                + "execution_date,execution_time,status) "
                + "VALUES (?,?,?,?,?,?);";
        // prepare statement
        PreparedStatement preparedInsertStatement = session.prepare(insertStatement);
        session.execute(preparedInsertStatement.bind(version,
                filename,
                checksum,
                new Timestamp(new Date().getTime()),
                execution_time,
                status
                ));

        log.trace("Leaving appendVersionRecord()");
    }

    /**
     * Retrive record for a given versionId
     * 
     * @param versionId
     * @return Result set with one row if versionId present, empty otherwise
     */
    public static ResultSet getVersionRecordByVersionId(Session session, String versionId) {
        log.trace("getting version record for id {}", versionId);
        String selectStatement = "SELECT * FROM \""
                + "Version"
                + "\" WHERE versionid = '" + versionId + "'";
        return session.execute(selectStatement);
    }

    /**
     * delete version record.
     * 
     * @param session
     *            - the session to execute cql.
     * @param versionId
     *            - version id.
     */
    public static void deleteVersionRecord(Session session, String versionId) {
        log.trace("deleting version record for id {}", versionId);

        String deleteStatement = "DELETE FROM \"Version\" WHERE versionid = '" + versionId + "';";
        session.execute(deleteStatement);
    }

    /**
     * delete all failed version records.
     * 
     * @param session
     *            - the session to execute cql.
     */
    public static void deleteFailedVersionRecord(Session session) {
        log.trace("Entering deleteFailedVersionRecord(session={})", session);

        ResultSet rs = getVersionRecords(session);
        List<Row> selectedRows = new ArrayList<Row>();

        while (!rs.isExhausted()) {
            Row r = rs.one();
            if (r.getString("status").equals(MutationStatus.FAILED.getValue())) {
                log.info("The following record has been selected for deletion : {}", r);
                selectedRows.add(r);
            }
        }

        log.info("The following record has been selected for deletion : {}", selectedRows);

        for (Row r : selectedRows) {
            log.trace("deleting row {}", r);
            deleteVersionRecord(session, r.getString("versionid"));
        }
    }

    /**
     * Check if the versionId exists in the database.
     * 
     * @return true if versionId is in the database
     */
    public static boolean isVersionIdPresent(Session session, String versionId) {
        log.trace("Executing isVersionIdPresent(session={}, versionId={})", session, versionId);
        return !getVersionRecordByVersionId(session, versionId).isExhausted();
    }

    /**
     * Check if the mutation fails.
     * 
     * @return true if versionId is in the database
     */
    public static boolean isMutationFailed(Session session, String versionId) {
        // get rows for given version id
        List<Row> rows = getVersionRecordByVersionId(session, versionId).all();

        boolean hasFailed = false;

        // if there's one and only one row, and the mutation has failed
        if (rows.size() == 1 && rows.get(0).getString("status").equals(MutationStatus.FAILED.getValue()))
            hasFailed = true;

        log.trace("Executed isVersionIdPresent(session={}, versionId={}) : {}", session, versionId, hasFailed);

        return hasFailed;
    }

    /**
     * check if the mutation checksum changes.
     * 
     * @param session
     *            - the session to execute cql.
     * @param versionId
     *            - version id.
     * @param hash
     *            - mutation hash.
     * @return bool
     */
    public static boolean isMutationHashCorrect(Session session, String versionId, String hash) {
        log.trace("Executing isMutationHashCorrect(session={}, versionId={}, hash={}", session, versionId, hash);

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
    public static String getCurrentState(Session session) {
        log.trace("Entering getCurrentState(session={})", session);

        String version = "000000000000";
        ResultSet results = null;
        try {
            results = getVersionRecords(session);
        } catch (Exception e) {
            log.trace("Failed to get version records, trying to create version table");
            try {
                createSchemaVersionTable(session);
                log.trace("creationg successful, second try to get version record");

                try {
                    results = getVersionRecords(session);
                } catch (Exception e3) {
                    throw new MutagenException(
                            "could not retreive Version table information", e);

                }
            } catch (Exception e2) {
                throw new MutagenException("Could not create version table", e2);
            }
        }

        while (!results.isExhausted()) {
            Row r = results.one();
            log.trace("Parsing row {}", r);

            String versionid = r.getString("versionid");
            if ((!r.getString("status").equals(MutationStatus.FAILED.getValue())) && version.compareTo(versionid) < 0)
                version = versionid;
        }

        log.trace("Leaving getCurrentState() : {}", version);

        return version;
    }
}