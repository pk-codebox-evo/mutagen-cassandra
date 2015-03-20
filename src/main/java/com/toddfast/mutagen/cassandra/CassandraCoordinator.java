package com.toddfast.mutagen.cassandra;

import com.datastax.driver.core.Session;
import com.toddfast.mutagen.Coordinator;
import com.toddfast.mutagen.MutagenException;
import com.toddfast.mutagen.State;
import com.toddfast.mutagen.Subject;

/**
 * An implementation of {@link Coordinator} that accepts all states
 * Whose timestamp is greater than the current database timestamp.
 * It acts as a filter.
 * 
 */
public class CassandraCoordinator implements Coordinator<String> {

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

        State<String> currentState = subject.getCurrentState();

        if (targetState.getID().compareTo(currentState.getID()) > 0) {
            return true;
        }
        else if (((CassandraSubject) subject).isVersionIdPresent(targetState.getID())) {
            return false;
        }
        else {
            throw new MutagenException(
                    "Mutation has state (state=" + targetState.getID() + ")" + " inferior to current state (state="
                            + currentState.getID() + ") but was not recorded in the database");
        }
    }

    // //////////////////////////////////////////////////////////////////////////
    // Fields
    // //////////////////////////////////////////////////////////////////////////

    private Session session; // session

}
