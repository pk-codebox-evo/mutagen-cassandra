package com.toddfast.mutagen.cassandra.impl;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import com.datastax.driver.core.Session;
import com.toddfast.mutagen.MutagenException;
import com.toddfast.mutagen.Plan;
import com.toddfast.mutagen.Plan.Result;
import com.toddfast.mutagen.Planner;
import com.toddfast.mutagen.cassandra.CassandraCoordinator;
import com.toddfast.mutagen.cassandra.CassandraMutagen;
import com.toddfast.mutagen.cassandra.CassandraSubject;
import com.toddfast.mutagen.cassandra.impl.baseline.BaseLine;
import com.toddfast.mutagen.cassandra.impl.info.MigrationInfoService;
import com.toddfast.mutagen.cassandra.impl.info.MigrationInfoServiceImpl;
import com.toddfast.mutagen.cassandra.util.DBUtils;
import com.toddfast.mutagen.cassandra.util.LoadResources;
import com.toddfast.mutagen.cassandra.util.logging.Log;
import com.toddfast.mutagen.cassandra.util.logging.LogFactory;

/**
 * An implementation for cassandraMutagen.
 * It execute all the migration tasks.
 * It is the enter point of application.
 */
public class CassandraMutagenImpl implements CassandraMutagen {
    private Session session;

    private static Log log = LogFactory.getLog(CassandraMutagenImpl.class);

    private List<String> resources;

    private String baselineVersion = "000000000000";

    private String location = "mutations";

    private MigrationInfoService migrationInfoService;

    public CassandraMutagenImpl(Session session) {
        this.session = session;
    }
    /**
     * Loads the resources.
     * 
     */
    public void initialize()
            throws IOException {
        log.debug("Initialising with resourcePath {}", location);
        resources = LoadResources.loadResources(this, location);

    }

    /**
     * Return the resources founded.
     *
     * @return resources
     */
    private List<String> getResources() {
        return resources;
    }

    /**
     * getter for baseline.
     * 
     * @return baseline version.
     */
    public String getBaselineVersion() {
        return baselineVersion;
    }

    /**
     * setter for baseline.
     * 
     * @param baselineVersion
     *            - baseline version.
     */
    public void setBaselineVersion(String baselineVersion) {
        this.baselineVersion = baselineVersion;
    }

    /**
     * getter for location.
     * 
     * @return location.
     */
    public String getLocation() {
        return location;
    }

    /**
     * setter for location.
     * 
     * @param location
     *            - location.
     */
    public void setLocation(String location) {
        this.location = location;
    }

    /**
     * Configure the properties of migration.
     */
    @Override
    public void configure(Properties properties) {
        String baselineVersion = properties.getProperty("baselineVersion");

        if (baselineVersion != null) {
            setBaselineVersion(baselineVersion);
        }

        String location = properties.getProperty("location");
        if (location != null) {
            setLocation(location);
        }

    }
    /**
     * Retrive a plan of mutations.
     * 
     * @return mutations plan
     */
    public Plan<String> getMutationsPlan() {
        log.trace("Entering getMutationsPlan(session={})", session);
        CassandraCoordinator coordinator = new CassandraCoordinator(session);
        CassandraSubject subject = new CassandraSubject(session);

        Planner<String> planner =
                new CassandraPlanner(session, getResources());
        Plan<String> plan = planner.getPlan(subject, coordinator);

        log.trace("Leaving getMutationsPlan() : {}", plan);
        return plan;
    }

    /**
     * Performs the automatic migration tasks.
     * 
     * @return
     *         the results of migration.
     */
    @Override
    public Plan.Result<String> mutate() {
        log.trace("Entering mutate(session={})", session);
        Result<String> mutationsResult;


        // Do this in a VM-wide critical section. External cluster-wide
        // synchronization is going to have to happen in the coordinator.
        synchronized (System.class) {

            mutationsResult = getMutationsPlan().execute();
        }

        log.trace("Leaving mutate()", mutationsResult);
        return mutationsResult;

    }

    /**
     * Drop version table.
     */
    @Override
    public void clean() {
        System.out.println("Cleaning...");

        // TRUNCATE instead of drop ?
        DBUtils.dropSchemaVersionTable(session);

        System.out.println("Done");


    }

    /**
     * delete the failed records in the version table.
     */
    @Override
    public void repair() {
        System.out.println("Repairing...");

        DBUtils.deleteFailedVersionRecord(session);

        System.out.println("Done");

    }

    /**
     * baseline
     */
    @Override
    public void baseline() throws MutagenException {

        System.out.println("Baseline...");

        synchronized (System.class) {
            new BaseLine(this, session, baselineVersion).baseLine();
        }
        System.out.println("Done with baseline");

    }

    /**
     * Retrives the complete information about all the migrations.
     * 
     * @param session
     *            - the session to execute the cql.
     * @return instance of MigrationInfoService.
     */
    public MigrationInfoService info() {
        if (migrationInfoService == null)
            return new MigrationInfoServiceImpl(session);
        else
            return migrationInfoService;
    }
}
