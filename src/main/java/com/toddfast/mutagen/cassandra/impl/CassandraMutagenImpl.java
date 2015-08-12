package com.toddfast.mutagen.cassandra.impl;

import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;
import com.toddfast.mutagen.MutagenException;
import com.toddfast.mutagen.Plan;
import com.toddfast.mutagen.Plan.Result;
import com.toddfast.mutagen.cassandra.CassandraCoordinator;
import com.toddfast.mutagen.cassandra.CassandraMutagen;
import com.toddfast.mutagen.cassandra.CassandraSubject;
import com.toddfast.mutagen.cassandra.impl.baseline.BaseLine;
import com.toddfast.mutagen.cassandra.impl.info.MigrationInfoService;
import com.toddfast.mutagen.cassandra.impl.info.MigrationInfoServiceImpl;
import com.toddfast.mutagen.cassandra.util.DBUtils;
import com.toddfast.mutagen.cassandra.util.LoadResources;

/**
 * An implementation for cassandraMutagen.
 * It execute all the migration tasks.
 * It is the enter point of application.
 */
public class CassandraMutagenImpl extends CassandraMutagen {

    private static Logger LOGGER = LoggerFactory.getLogger(CassandraMutagenImpl.class);

    private MigrationInfoService migrationInfoService;

    public CassandraMutagenImpl(Session session) {
        super(session);
    }

    /**
     * Loads the resources.
     * 
     */
    public void initialize()
            throws IOException {
        LOGGER.debug("Initialising with resourcePath {}", getLocation());
        setResources(LoadResources.loadResources(this, getLocation(), getResourceScannerPatternFilter()));

    }

    /**
     * Configure the properties of migration.
     */
    @Override
    public void configure(Properties properties) {
        // get baseline
        String baselineVersion = properties.getProperty("baselineVersion");
        if (baselineVersion != null) {
            setBaselineVersion(baselineVersion);
        }
        // get location
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
    public Plan<String> getMutationsPlan(boolean ignoreDB) {
        LOGGER.trace("Entering getMutationsPlan(session={})", getSession());
        CassandraCoordinator coordinator = new CassandraCoordinator(getSession());
        CassandraSubject subject = new CassandraSubject(getSession());

        CassandraPlanner planner = new CassandraPlanner(getSession(), getResources());
        planner.setIgnoreDB(ignoreDB);
        Plan<String> plan = planner.getPlan(subject, coordinator);

        LOGGER.trace("Leaving getMutationsPlan(session={})", getSession());
        return plan;
    }

    /**
     * Performs the automatic migration tasks.
     * 
     * @return
     *         the results of migration.
     */
    @Override
    public Plan.Result<String> mutate(boolean ignoreDB) {
        LOGGER.trace("Entering mutate(session={})", getSession());
        Result<String> mutationsResult;

        // Do this in a VM-wide critical section. External cluster-wide
        // synchronization is going to have to happen in the coordinator.
        synchronized (System.class) {
            Plan<String> plan = getMutationsPlan(ignoreDB);
            mutationsResult = plan.execute();
        }

        LOGGER.trace("Leaving mutate()", mutationsResult);
        return mutationsResult;

    }

    @Override
    public Plan.Result<String> migrate(String path) {
        Plan.Result<String> result = this.migrate(path, false);

        return result;
    }

    @Override
    public Plan.Result<String> migrate(String path, boolean ignoreDB) {
        this.setLocation(path);
        // load migration scripts
        try {
            this.initialize();
        } catch (IOException e) {
            throw new RuntimeException("Can not load scripts");
        }
        // migrate
        Plan.Result<String> result = null;
        try {
            result = this.mutate(ignoreDB);
        } catch (Exception e) {
            System.out.println("ERROR:" + e.getMessage());
        }

        return result;
    }

    @Override
    public Plan.Result<String> withInitScript(String path) {
        Plan.Result<String> result = this.migrate(path, true);

        return result;
    }

    /**
     * Drop version table.
     */
    @Override
    public void clean() {
        System.out.println("Cleaning...");

        DBUtils.dropSchemaVersionTable(getSession());

        System.out.println("Done");

    }

    /**
     * delete the failed records in the version table.
     */
    @Override
    public void repair() {
        System.out.println("Repairing...");

        DBUtils.deleteFailedVersionRecord(getSession());

        System.out.println("Done");

    }

    /**
     * baseline
     */
    @Override
    public void baseline() throws MutagenException {

        System.out.println("Baseline...");

        synchronized (System.class) {
            if (getBaselineVersion() != null) {
                new BaseLine(this, getSession(), getBaselineVersion()).baseLine();
            } else {
                new BaseLine(this, getSession()).baseLine();
            }
        }
        System.out.println("Done with baseline");

    }

    /**
     * Retrives the complete information about all the migrations.
     * 
     * @param getSession
     *            ()
     *            - the getSession() to execute the cql.
     * @return instance of MigrationInfoService.
     * 
     */
    public MigrationInfoService info() {
        if (migrationInfoService == null) {
            migrationInfoService = new MigrationInfoServiceImpl(getSession());
        }
        return migrationInfoService;
    }
}
