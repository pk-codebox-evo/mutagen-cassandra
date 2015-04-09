package com.toddfast.mutagen.cassandra.impl;

import java.io.IOException;
import java.util.List;

import com.datastax.driver.core.Session;
import com.toddfast.mutagen.MutagenException;
import com.toddfast.mutagen.Plan;
import com.toddfast.mutagen.Planner;
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
public class CassandraMutagenImpl implements CassandraMutagen {

    private List<String> resources;

    /**
     * Loads the resources.
     * 
     */
    @Override
    public void initialize(String rootResourcePath)
            throws IOException {
        resources = LoadResources.loadResources(rootResourcePath, this);
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
     * Retrive a plan of mutations.
     * 
     * @return mutations plan
     */
    private Plan<String> getMutationsPlan(Session session) {
        CassandraCoordinator coordinator = new CassandraCoordinator(session);
        CassandraSubject subject = new CassandraSubject(session);

        Planner<String> planner =
                new CassandraPlanner(session, getResources());
        Plan<String> plan = planner.getPlan(subject, coordinator);

        return plan;
    }

    /**
     * Performs the automatic migration tasks.
     * 
     * @return
     *         the results of migration.
     */
    @Override
    public Plan.Result<String> mutate(Session session) {
        // Do this in a VM-wide critical section. External cluster-wide
        // synchronization is going to have to happen in the coordinator.

        synchronized (System.class) {
            return getMutationsPlan(session).execute();
        }
    }

    /**
     * Drop version table.
     */
    @Override
    public void clean(Session session) {
        System.out.println("Cleaning...");
        // TRUNCATE instead of drop ?
        DBUtils.dropSchemaVersionTable(session);
        System.out.println("Done");

    }

    /**
     * delete the failed records in the version table.
     */
    @Override
    public void repair(Session session) {
        System.out.println("Repairing...");
        DBUtils.deleteFailedVersionRecord(session);
        System.out.println("Done");

    }

    /**
     * baseline
     */
    @Override
    public void baseline(Session session, String lastCompletedState) throws MutagenException {

        System.out.println("Baseline...");

        synchronized (System.class) {
            Plan<String> mutationsPlan = getMutationsPlan(session);
            new BaseLine(session, lastCompletedState, mutationsPlan).baseLine();
        }
        System.out.println("Done");

    }

    /**
     * Retrives the complete information about all the migrations.
     * 
     * @param session
     *            - the session to execute the cql.
     * @return instance of MigrationInfoService.
     */
    public MigrationInfoService info(Session session) {
        return new MigrationInfoServiceImpl(session);
    }

}
