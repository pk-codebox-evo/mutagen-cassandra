package com.toddfast.mutagen.cassandra.impl.baseline;

import com.datastax.driver.core.Session;
import com.toddfast.mutagen.MutagenException;
import com.toddfast.mutagen.Mutation;
import com.toddfast.mutagen.cassandra.AbstractCassandraMutation;
import com.toddfast.mutagen.cassandra.CassandraMutagen;
import com.toddfast.mutagen.cassandra.util.DBUtils;

public class BaseLine {
    // Fields
    private Session session;

    private String baselineVersion;

    private CassandraMutagen mutagen;

    // Methods
    public BaseLine(CassandraMutagen mutagen, Session session) {
        this.mutagen = mutagen;
        this.session = session;
        this.baselineVersion = "000000000001";
    }

    public BaseLine(CassandraMutagen mutagen, Session session, String baselineVersion) {
        this.mutagen = mutagen;
        this.session = session;
        this.baselineVersion = baselineVersion;
    }

    public void baseLine() throws MutagenException {
        DBUtils.createSchemaVersionTable(session);
        if (!DBUtils.isEmptyVersionTable(session))
            throw new MutagenException("Tabble Version is not empty, please clean before executing baseline");
        dummyPlanExecution();
    }

    // Dummy execution of all mutations with state inferior of equal to lastCompletedState
    private void dummyPlanExecution() {

        for (Mutation<String> m : mutagen.getMutationsPlan().getMutations()) {
            try {
                ((AbstractCassandraMutation) m).dummyExecution(baselineVersion);
            } catch (Exception e) {
                throw new MutagenException("Dummy execution failed for mutation : " + m.toString(), e);
            }
        }
        if (!DBUtils.isVersionIdPresent(session, baselineVersion)) {
            DBUtils.appendVersionRecord(session, baselineVersion, "", "", 0, "Baseline");
        }
    }

}
