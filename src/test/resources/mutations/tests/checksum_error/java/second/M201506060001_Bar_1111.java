package mutations.tests.checksum_error.java.second;

import com.toddfast.mutagen.cassandra.impl.JavaMutation;

public class M201506060001_Bar_1111 extends JavaMutation {

    @Override
    protected void performMutation(com.toddfast.mutagen.Mutation.Context context) {

        getSession().execute("CREATE TABLE \"Test1\" (key varchar PRIMARY KEY,value1 varchar);");

    }

}
