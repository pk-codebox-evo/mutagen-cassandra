package mutations.tests.repair;

import com.toddfast.mutagen.cassandra.impl.JavaMutation;

// First script, successful
public class M201501010001_Bar_1111 extends JavaMutation {

    @Override
    protected void performMutation(Context context) {

        getSession().execute("CREATE TABLE \"Test1\" (key varchar PRIMARY KEY,value1 varchar);");

    }

}
