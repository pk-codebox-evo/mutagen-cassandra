package com.toddfast.mutagen.cassandra;

import com.datastax.driver.core.Session;
import com.toddfast.mutagen.Coordinator;
import com.toddfast.mutagen.State;
import com.toddfast.mutagen.Subject;
import com.toddfast.mutagen.cassandra.util.logging.Log;
import com.toddfast.mutagen.cassandra.util.logging.LogFactory;

/**
 * An implementation of {@link Coordinator} that accepts all states
 * Whose timestamp is greater than the current database timestamp.
 * It acts as a filter.
 * 
 */
public class CassandraCoordinator implements Coordinator<String> {
    // //////////////////////////////////////////////////////////////////////////
    // Fields
    // //////////////////////////////////////////////////////////////////////////

    private Log log = LogFactory.getLog(CassandraCoordinator.class);
    private Session session; // session
    /**
     * Constructor for cassandra coordinator.
     * 
     * @param session
     *            the session to execute cql statements
     */
    public CassandraCoordinator(Session session) {
        super();
        if (session == null) {
            throw new IllegalArgumentException(
                    "Parameter \"session\" cannot be null");
        }

        this.session = session;
    }

    /**
     * A getter method to get session.
     * 
     * @return session
     */
    public Session getSession() {
        return session;
    }

    /**
     * Return if the timestamp of state is greater than the current database timestamp.
     * 
     * @return
     *         true or false
     */
    @Override
    public boolean accept(Subject<String> subject,
            State<String> targetState) {
        log.trace("Entering  accept(subject={}, targetState={})", subject, targetState);

        State<String> currentState = subject.getCurrentState();

        // accept if the target state is superior to the current one.
        boolean isAccepted = targetState.getID().compareTo(currentState.getID()) > 0;

        log.trace("Leaving  accept() : {}", isAccepted);

        return isAccepted;

    }
}
