package mutations.tests.failed_java_mutation.failed_cql_mutation;

import com.toddfast.mutagen.cassandra.impl.JavaMutation;

// Second script, fails
public class M201501010002_WrongJavaScriptFile_1111 extends JavaMutation {

    @Override
    protected void performMutation(Context context) {
        throw new RuntimeException("this java script 'M201506070001_WrongJavaScriptFile_1111' always fails");
    }

}
