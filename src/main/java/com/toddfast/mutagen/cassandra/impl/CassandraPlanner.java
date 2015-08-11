package com.toddfast.mutagen.cassandra.impl;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;
import com.toddfast.mutagen.Coordinator;
import com.toddfast.mutagen.MutagenException;
import com.toddfast.mutagen.Mutation;
import com.toddfast.mutagen.Plan;
import com.toddfast.mutagen.State;
import com.toddfast.mutagen.Subject;
import com.toddfast.mutagen.cassandra.AbstractCassandraMutation;
import com.toddfast.mutagen.cassandra.util.DBUtils;

/**
 * Generates cassandra migration plans using the initial list of mutations and
 * the specified subject and coordinator.
 */
public class CassandraPlanner extends BasicPlanner<String> {

    private static Logger LOGGER = LoggerFactory.getLogger(CassandraPlanner.class);

    private final static String FILENAME_PATTERN = "M\\d{12}_[\\w\\d\\-]+_[\\w\\d\\-]+\\.(class|cqlsh\\.txt)";

    private Session session;

    /**
     * Constructor for cassandraPlanner.
     * 
     * @param session
     *            the session to execute cql statements.
     * @param mutationResources
     *            script files to migrate.
     * 
     */
    protected CassandraPlanner(Session session,
            List<String> mutationResources) {
        super(loadMutations(session, mutationResources), null);
        this.session = session;
    }

    /**
     * A static method to load mutation for script file(.cqlsh.txt and .java).
     * 
     * @param session
     *            the session to execute cql statements.
     * @param resources
     *            script files to mutate.
     * @return
     *         list of mutation objects.
     */
    private static List<Mutation<String>> loadMutations(
            Session session, Collection<String> resources) {

        LOGGER.trace("Entering loadMutations(session={}, resources={})", session, resources);

        List<Mutation<String>> result = new ArrayList<>();

        for (String resource : resources) {

            // Allow .sql files because some editors have syntax highlighting
            // for SQL but not CQL
            if (resource.endsWith(".cqlsh.txt") || resource.endsWith(".sql")) {
                result.add(new CqlMutation2(session, resource));
            }
            else if (resource.endsWith(".class")) {
                result.add(loadMutationClass(session, resource));
            } else if (resource.endsWith(".java")) {
                // ignore java file
                if (!hasJavaFileAssociatedClassFile(resource, resources)) {
                    throw new IllegalArgumentException("Java file found without corresponding class file for resource \"" + resource + "\".");
                }
                continue;
            } else {
                throw new IllegalArgumentException("Unknown type for resource \"" + resource + "\".");
            }


            // check name of script file
            if (!validate(resource)) {
                throw new IllegalArgumentException("wrong name for " +
                        "mutation resource \"" + resource + "\"");
            }
        }

        checkForDuplicateRessourceState(result);

        LOGGER.trace("Leaving loadMutations() : {}", result);

        return result;
    }

    private static boolean hasJavaFileAssociatedClassFile(String javaResource, Collection<String> resources) {
        String classResource = javaResource.replaceAll("(.*)java", "$1class");
        for (String resource : resources) {
            if (resource.equals(classResource)) {
                return true;
            }
        }
        return false;
//        String classResource = javaResource.substring(0, javaResource.length() - "java".length()) + "class";
    }

    /**
     * Check if it exists two mutations which have the same state.
     * 
     * @param mutations
     *            the list of mutations.
     */
    private static void checkForDuplicateRessourceState(List<Mutation<String>> mutations) {

        LOGGER.trace("checking for duplicate resource states");
        // store all states as string
        List<String> states = new ArrayList<>();

        for (Mutation<String> m : mutations) {
            states.add(m.getResultingState().getID());
        }

        // create set to get duplicate state.
        Set<String> set = new HashSet<String>();
        ArrayList<String> duplicateResourceState = new ArrayList<String>();
        for (String state : states) {
            if (!set.add(state)) {
                duplicateResourceState.add(state);
            }
        }

        // if sizes differ there's a duplicate
        if (duplicateResourceState.size() > 0)
            throw new MutagenException("Two migration scripts possess the same state" + duplicateResourceState);

    }

    /**
     * validate if the script file is well named(
     * M<DATETIME>_<Camel case title>_<ISSUE>.cqlsh.txt or
     * M<DATETIME>_<Camel case title>_<ISSUE>.java)
     * 
     * @return
     */
    private static boolean validate(String resource) {
        String filename = Paths.get(resource).getFileName().toString();
        return filename.matches(FILENAME_PATTERN);
    }

    /**
     * a static method to generate the mutation for script file end with .class
     * 
     * @return
     *         mutation for script file end with .class
     */
    private static Mutation<String> loadMutationClass(Session session, String resource) {

        LOGGER.trace("Entering loadMutationClass(session={}, resource={})", session, resource);

        assert resource.endsWith(".class") : "Class resource name \"" + resource + "\" should end with .class";

        int index = resource.indexOf(".class");
        String className = resource.substring(0, index).replace('/', '.');

        // Load the class specified by the resource
        Class<?> clazz;
        try {
            clazz = Class.forName(className);
        } catch (ClassNotFoundException e) {
            // Should never happen
            throw new MutagenException("Could not load mutagen class \"" + resource + "\"", e);
        }

        // Instantiate the class
        try {
            Constructor<?> constructor;
            Mutation<String> mutation;
            try {
                LOGGER.debug("Instanciating {}", clazz);

                constructor = clazz.getConstructor();
                mutation = (Mutation<String>) constructor.newInstance();

                LOGGER.debug("Setting session {} for {}", session, clazz);
                // Assumption that the mutation must extend AbstractCassandraMutation, then set session
                ((AbstractCassandraMutation) mutation).setSession(session);

            } catch (NoSuchMethodException e) {
                throw new MutagenException("Could not find compatible " +
                        "constructor for class \"" + className + "\"", e);
            }

            LOGGER.trace("Leaving loadMutationClass() : {}", mutation);

            return mutation;
        } catch (InstantiationException e) {
            throw new MutagenException("Could not instantiate class \"" + className + "\"", e);
        } catch (InvocationTargetException e) {
            if (e.getTargetException() instanceof RuntimeException) {
                throw (RuntimeException) e.getTargetException();
            } else {
                throw new MutagenException("Exception instantiating class \"" + className + "\"", e);
            }
        } catch (IllegalAccessException e) {
            throw new MutagenException("Could not access constructor for " + "mutation class \"" + className + "\"", e);
        }
    }

    /**
     * generate mutation context to execute mutations.
     * 
     * @return
     *         mutation context
     */
    @Override
    protected Mutation.Context createContext(Subject<String> subject,
            Coordinator<String> coordinator) {
        return new CassandraContext(subject, coordinator);
    }

    /**
     * generate cassandra migration plan for
     * mutating the target subject through a sequence of states.
     * 
     * @return
     *         mutation plan.
     */
    @Override
    public Plan<String> getPlan(Subject<String> subject,
            Coordinator<String> coordinator) {

        LOGGER.trace("Entering getPlan(subject={}, coordinator={})", subject, coordinator);

        List<Mutation<String>> subjectMutations =
                new ArrayList<Mutation<String>>(getMutations());

        // Filter out the mutations that are unacceptable to the subject
        for (Iterator<Mutation<String>> i = subjectMutations.iterator(); i.hasNext();) {

            Mutation<String> mutation = i.next();
            State<String> targetState = mutation.getResultingState();
            LOGGER.debug("Evaluating mutation {}", mutation);
            // Check by state first
            if (!coordinator.accept(subject, targetState)) {

                // For older states, verify its presence in the database
                if (DBUtils.isVersionIdPresent(session, targetState.getID())) {

                    // Check that the md5 hash of the already executed mutation hasn't changed
                    if (DBUtils.isMutationHashCorrect(session, targetState.getID(),
                            ((AbstractCassandraMutation) mutation).getChecksum())) {
                        LOGGER.debug("Rejecting mutation {}", mutation);
                        i.remove();
                    }
                    else
                        throw new MutagenException("Checksum incorrect for already executed mutation : "
                                + targetState);
                }
                else {
                    throw new MutagenException(
                            "Mutation has state (state=" + targetState.getID() + ")"
                                    + " inferior to current state (state="
                                    + subject.getCurrentState().getID() + ") but was not recorded in the database");
                }

            }
            
            //Test that the mutation hasn't been executed with errors before
            if (DBUtils.isMutationFailed(session, targetState.getID()))
                    throw new MutagenException("There is a failed mutation in database for script : " + mutation.toString());
        }

        BasicPlan basicPlan = new BasicPlan(subject, coordinator, subjectMutations);
        LOGGER.trace("Leaving getPlan(subject={}, coordinator={})", subject, coordinator);

        return basicPlan;
    }
}
