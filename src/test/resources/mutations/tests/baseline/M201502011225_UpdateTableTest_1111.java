package mutations.tests.baseline;


import com.toddfast.mutagen.MutagenException;
import com.toddfast.mutagen.cassandra.impl.JavaMutation;

/**
 *
 * It is a script file java that update table Test1.
 */
public class M201502011225_UpdateTableTest_1111 extends JavaMutation {



    // Mutation with error to make sure the base line never executes it

    @Override
    protected void performMutation(Context context) {
        context.debug("Executing mutation {}", getResultingState().getID());

        try {
            String updateStatement = "updazeazeazeaate \"Test1\" set value1='chicken', value2='sneeze' " +
                    "where key='row2';";
            getSession().execute(updateStatement);
        } catch (Exception e) {
            throw new MutagenException("Could not update columnfamily Test1", e);
        }
    }


}
