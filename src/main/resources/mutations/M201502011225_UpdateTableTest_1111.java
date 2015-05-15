package mutations;

import com.toddfast.mutagen.MutagenException;
import com.toddfast.mutagen.cassandra.impl.JAVAMutation;

/**
 * 
 * It is a script file java.
 * It is just for test.
 */
public class M201502011225_UpdateTableTest_1111 extends JAVAMutation {

    @Override
    protected void performMutation(Context context) {
        context.debug("Executing mutation {}", getResultingState().getID());

        try {
            String updateStatement = "update \"Test1\" set value1='chicken', value2='sneeze' " +
                    "where key='row2';";
            getSession().execute(updateStatement);
            context.debug("Done executing mutation {}", getResultingState().getID());
        } catch (Exception e) {
            throw new MutagenException("Could not update columnfamily Test1", e);
        }
    }


}
