package com.toddfast.mutagen.cassandra.impl.info;

import java.util.Date;

public interface MigrationInfo {
    /**
     * @return the timestamp of migration.
     * 
     */
    String getVersion();

    /**
     * @return the date when the migration was executed.
     * 
     */
    Date getDate();

    /**
     * @return the filename of the script migration.
     * 
     */
    String getFilename();

    /**
     * @return if the migration success or not.
     * 
     */
    String getStatus();

    /**
     * @return the string representation of migration.
     * 
     */
    String toString();
}
