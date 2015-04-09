package com.toddfast.mutagen.cassandra.impl.baseline;

import com.datastax.driver.core.Session;
import com.toddfast.mutagen.MutagenException;
import com.toddfast.mutagen.Mutation;
import com.toddfast.mutagen.Plan;
import com.toddfast.mutagen.cassandra.AbstractCassandraMutation;
import com.toddfast.mutagen.cassandra.util.DBUtils;

public class BaseLine {
    // Fields
    private Session session;

    private String lastCompletedState;

    private Plan<String> plan;


    // Methods
    public BaseLine(Session session, String lastCompletedState, Plan<String> plan) {
        this.session = session;
        this.lastCompletedState = lastCompletedState;
        this.plan = plan;
    }

    public void baseLine() throws MutagenException {
        DBUtils.createSchemaVersionTable(session);
        if (!DBUtils.isEmptyVersionTable(session))
            throw new MutagenException("Tabble Version is not empty, please clean before executing baseline");
        dummyPlanExecution(lastCompletedState, plan);
    }


    // Dummy execution of all mutations with state inferior of equal to lastCompletedState
    private void dummyPlanExecution(String lastCompletedState, Plan<String> plan) {

        for (Mutation<String> m : plan.getMutations()) {

            if (m.getResultingState().getID().compareTo(lastCompletedState) <= 0)
                try {
                    ((AbstractCassandraMutation) m).dummyExecution();
                } catch (Exception e) {
                    throw new MutagenException("Dummy execution failed for mutation : " + m.toString(), e);
                }
        }

    }

}
