package mutations.tests.wrongJavaScript;


import com.toddfast.mutagen.MutagenException;
import com.toddfast.mutagen.cassandra.impl.JavaMutation2;

/**
 *
 * It is a script file java with wrong cql statements.
 * 
 */
public class M201502011225_WrongJavaScriptFile_1111 extends JavaMutation2 {

    @Override
    protected void performMutation(Context context) {
        context.debug("Executing mutation {}", getResultingState().getID());

        try {
            String createTableStatement = "create table \"Test1\"();";
            getSession().execute(createTableStatement);
        } catch (Exception e) {
            throw new MutagenException("Could not create table Test1", e);
        }
    }

}
