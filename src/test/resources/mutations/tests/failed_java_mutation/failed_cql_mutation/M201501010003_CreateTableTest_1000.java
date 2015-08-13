package mutations.tests.failed_java_mutation.failed_cql_mutation;

import com.toddfast.mutagen.cassandra.impl.JavaMutation;

// Third script, successful
public class M201501010003_CreateTableTest_1000 extends JavaMutation {
    @Override
    protected void performMutation(Context context) {
        getSession().execute("insert into \"Test1\" (key, value1) values ('row1', 'value1');");
    }
}
