package com.toddfast.mutagen.cassandra.impl.baseline;

import com.datastax.driver.core.Session;
import com.toddfast.mutagen.MutagenException;
import com.toddfast.mutagen.Mutation;
import com.toddfast.mutagen.cassandra.AbstractCassandraMutation;
import com.toddfast.mutagen.cassandra.CassandraMutagen;
import com.toddfast.mutagen.cassandra.MutationStatus;
import com.toddfast.mutagen.cassandra.utils.DBUtils;

public class BaseLine {
    // Fields
    private Session session;

    private String baselineVersion;

    private CassandraMutagen mutagen;

    // Methods
    public BaseLine(CassandraMutagen mutagen, Session session) {
        this(mutagen, session, "000000000001");
    }

    public BaseLine(CassandraMutagen mutagen, Session session, String baselineVersion) {
        this.setMutagen(mutagen);
        this.setSession(session);
        this.setBaselineVersion(baselineVersion);
    }

    public void baseLine() throws MutagenException {
        DBUtils.createSchemaVersionTable(getSession());
        if (!DBUtils.isEmptyVersionTable(getSession()))
            throw new MutagenException("Tabble Version is not empty, please clean before executing baseline");
        dummyPlanExecution();
    }

    // Dummy execution of all mutations with state inferior of equal to lastCompletedState
    private void dummyPlanExecution() {

        for (Mutation<String> m : getMutagen().getMutationsPlan(false).getMutations()) {
            try {
                dummyExecution((AbstractCassandraMutation) m);
            } catch (Exception e) {
                throw new MutagenException("Dummy execution failed for mutation : " + m.toString(), e);
            }
        }
        if (!DBUtils.isVersionIdPresent(getSession(), getBaselineVersion())) {
            DBUtils.appendVersionRecord(getSession(), getBaselineVersion(), "", "", 0, MutationStatus.BASELINE.getValue());
        }
    }

    /**
     * dummy execution.
     */
    public void dummyExecution(AbstractCassandraMutation mutation) {
        String version = mutation.getResultingState().getID();

        // caculate the checksum
        String checksum = mutation.getChecksum();

        // append version record
        if (version.compareTo(getBaselineVersion()) < 0) {
            DBUtils.appendVersionRecord(getSession(), version, mutation.getResourceName(), checksum, 0, MutationStatus.BEFORE_BASELINE.getValue());
        } else if (version.compareTo(getBaselineVersion()) == 0) {
            DBUtils.appendVersionRecord(getSession(), version, mutation.getResourceName(), checksum, 0, MutationStatus.BASELINE.getValue());
        }
    }

    // getter and setter
    public Session getSession() {
        return session;
    }

    public void setSession(Session session) {
        this.session = session;
    }

    public String getBaselineVersion() {
        return baselineVersion;
    }

    public void setBaselineVersion(String baselineVersion) {
        this.baselineVersion = baselineVersion;
    }

    public CassandraMutagen getMutagen() {
        return mutagen;
    }

    public void setMutagen(CassandraMutagen mutagen) {
        this.mutagen = mutagen;
    }

}
