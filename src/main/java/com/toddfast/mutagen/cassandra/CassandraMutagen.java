package com.toddfast.mutagen.cassandra;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import com.datastax.driver.core.Session;
import com.toddfast.mutagen.Plan;
import com.toddfast.mutagen.cassandra.impl.info.MigrationInfoService;

/**
 * An interface that runs the cassandra migration tasks.
 * The application should implement this interface to finish automatic migration.
 */
public abstract class CassandraMutagen {

    private Session session;

    private List<String> resources;

    private String baselineVersion;

    private String location;

    public CassandraMutagen(Session session) {
        this.setSession(session);
        this.setLocation("mutations");
    }

    /**
     * Configures cassandra mutagen with these properties.
     * It can overwrite any existing configuration.
     * 
     * @param properties
     *            - properties.
     */
    public abstract void configure(Properties properties);

    /**
     * 
     * @throws IOException
     *             IO Exception
     */
    public abstract void initialize()
            throws IOException;
    /**
     * Retrive a plan of mutations.
     * 
     * @return mutations plan
     */
    public abstract Plan<String> getMutationsPlan(boolean ignoreDB);

    /**
     * @return
     *         the result of migration of all the scripts.
     */
    public abstract Plan.Result<String> mutate(boolean ignoreDB);

    /**
     * @return
     *         the result of migration of all the scripts.
     */
    public abstract Plan.Result<String> migrate(String path, boolean ignoreDB);

    /**
     * @return
     *         the result of migration of all the scripts.
     */
    public abstract Plan.Result<String> withInitScript(String path);

    /**
     * @return
     *         the result of migration of all the scripts.
     */
    public abstract Plan.Result<String> migrate(String path);

    /**
     * Baseline.
     */
    public abstract void baseline();

    /**
     * Clean.
     */
    public abstract void clean();

    /**
     * Repair.
     */
    public abstract void repair();

    /**
     * Retrieve the complete information about all the migrations.
     * 
     * @return instance of MigrationInfoService.
     * 
     */
    public abstract MigrationInfoService info();

    // getter and setter for variables.
    /**
     * getter for session.
     * 
     * @return session.
     */
    public Session getSession() {
        return session;
    }

    /**
     * setter for session.
     * 
     * @param session
     *            - session.
     */
    public void setSession(Session session) {
        this.session = session;
    }

    /**
     * getter for resources.
     * 
     * @return resource list.
     */
    public List<String> getResources() {
        return resources;
    }

    /**
     * setter for resources.
     * 
     * @param resources
     *            - resources list.
     */
    public void setResources(List<String> resources) {
        this.resources = resources;
    }

    /**
     * getter for baseline.
     * 
     * @return baseline.
     */
    public String getBaselineVersion() {
        return baselineVersion;
    }

    /**
     * setter for baseline.
     * 
     * @param baselineVersion
     *            - baselineVersion.
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
     *            - resource location.
     */
    public void setLocation(String location) {
        this.location = location;
    }

}
