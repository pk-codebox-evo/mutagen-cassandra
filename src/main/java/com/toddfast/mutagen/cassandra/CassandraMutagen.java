package com.toddfast.mutagen.cassandra;

import java.io.IOException;
import java.util.Properties;

import com.toddfast.mutagen.Plan;
import com.toddfast.mutagen.cassandra.impl.info.MigrationInfoService;

/**
 * An interface that runs the cassandra migration tasks.
 * The application should implement this interface to finish automatic migration.
 */
public interface CassandraMutagen {
    /**
     * Configures cassandra mutagen with these properties.
     * It can overwrite any existing configuration.
     * 
     * @param properties
     *            - properties.
     */
    public void configure(Properties properties);

    /**
     * 
     * @param rootResourcePath
     *            the path where the scripts files(.cqlsh.txt and .java) are located.
     * @throws IOException
     *             IO Exception
     */
    public void initialize()
            throws IOException;

    /**
     * @return
     *         the result of migration of all the scripts.
     */
    public Plan.Result<String> mutate();

    /**
     *  
     */
    public void baseline();

    /**
     * 
     */
    public void clean();

    /**
     * 
     */
    public void repair();

    /**
     * Retrive the complete information about all the migrations.
     * 
     * @return instance of MigrationInfoService.
     * 
     */
    public MigrationInfoService info();

}
