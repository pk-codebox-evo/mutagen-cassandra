package com.toddfast.mutagen.cassandra;

import com.datastax.driver.core.Session;
import com.toddfast.mutagen.State;
import com.toddfast.mutagen.Subject;
import com.toddfast.mutagen.cassandra.impl.DBUtils;

/**
 * Cassandra subject represents the table Version.
 * It executes the tasks related with the table version: <br>
 * create version table <br>
 * query version table <br>
 * get the current datebase timestamp in the version table<br>
 * 
 */
public class CassandraSubject implements Subject<String> {

    /**
     * Constructor for cassandraSubjet.
     * 
     * @param session
     *            the session to execute cql statements.
     * 
     */
    public CassandraSubject(Session session) {
        super();
        if (session == null) {
            throw new IllegalArgumentException(
                    "Parameter \"keyspace\" cannot be null");
        }

        this.session = session;
    }

    @Override
    public State<String> getCurrentState() {
        return DBUtils.getCurrentState(session);
    }


    // //////////////////////////////////////////////////////////////////////////
    // Fields
    // //////////////////////////////////////////////////////////////////////////

    private Session session; // session

}
