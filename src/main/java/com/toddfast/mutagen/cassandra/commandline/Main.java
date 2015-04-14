package com.toddfast.mutagen.cassandra.commandline;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;
import com.toddfast.mutagen.MutagenException;
import com.toddfast.mutagen.Plan;
import com.toddfast.mutagen.cassandra.CassandraMutagen;
import com.toddfast.mutagen.cassandra.commandline.ConsoleLog.Level;
import com.toddfast.mutagen.cassandra.impl.CassandraMutagenImpl;
import com.toddfast.mutagen.cassandra.util.logging.Log;
import com.toddfast.mutagen.cassandra.util.logging.LogFactory;

public class Main {
    private static Log LOG;

    public static void main(String[] args) {
        Level logLevel = getLogLevel(args);
        initLogging(logLevel);
        try {
            List<String> operations = determineOperations(args);
            if (operations.isEmpty()) {
                printUsage();
                return;
            }

            Properties properties = new Properties();
            loadProperties(properties);
            overrideConfiguration(properties, args);

            Session session = createSession(properties);
            CassandraMutagenImpl mutagen = new CassandraMutagenImpl(session);
            mutagen.configure(properties);
            mutagen.initialize();

            for (String operation : operations) {
                executeOperation(mutagen, operation);
            }
            // close session and cluster
            if (session != null) {
                session.close();
                session.getCluster().close();
            }
        } catch (Exception e) {
            if (logLevel == Level.DEBUG) {
                LOG.error("Unexpected error", e);
            } else {
                if (e instanceof MutagenException) {
                    LOG.error(e.getMessage());
                } else {
                    LOG.error(e.toString());
                }
            }
            System.exit(1);
        }
    }

    private static void executeOperation(CassandraMutagen mutagen, String operation) {
        if ("clean".equals(operation)) {
            mutagen.clean();
        } else if ("baseline".equals(operation)) {
            mutagen.baseline();
        } else if ("migrate".equals(operation)) {
            showMigrationResults(mutagen.mutate());
        } else if ("info".equals(operation)) {
            mutagen.info().refresh();
            LOG.info("\n" + mutagen.info().toString());
        } else if ("repair".equals(operation)) {
            mutagen.repair();
        } else {
            LOG.error("Invalid operation: " + operation);
            printUsage();
            System.exit(1);
        }

    }

    /**
     * Show the result of migrations.
     * 
     * @param result
     *            - the result of migration.
     */
    private static void showMigrationResults(Plan.Result<String> result) {
        if (result.isMutationComplete())
            LOG.info("migration is finished.");
        else {
            LOG.info(result.getCompletedMutations().size() + " migrations are finished!");
            LOG.info(result.getRemainingMutations().size() + " migration are not finished!");
        }
    }

    /**
     * Create session.
     * 
     * @param properties
     *            - properties
     * @return
     */
    private static Session createSession(Properties properties) {
        String keyspace = getUsedProperty(properties, "keyspace");
        Cluster cluster = createCluster(properties);
        Session session = null;
        // create session
        if (keyspace != null)
            session = cluster.connect(keyspace);
        else
            session = cluster.connect();
        return session;

    }

    /**
     * Create cluster.
     * 
     * @param properties
     *            - properties.
     * @return
     */
    private static Cluster createCluster(Properties properties) {
        String clusterContactPoint, clusterPort, useCredentials, dbuser, dbpassword, keyspace;

        // get cluster builder
        Cluster.Builder clusterBuilder = Cluster.builder().withProtocolVersion(ProtocolVersion.V2);

        // set contact point
        if ((clusterContactPoint = getUsedProperty(properties, "clusterContactPoint")) != null)
            clusterBuilder = clusterBuilder.addContactPoint(clusterContactPoint);

        // set cluster port if given
        if ((clusterPort = getUsedProperty(properties, "clusterPort")) != null)
            try {
                clusterBuilder = clusterBuilder.withPort(Integer.parseInt(clusterPort));
            } catch (NumberFormatException e) {
                System.err.println("Port parameter must be an integer");
                e.printStackTrace();
                System.exit(1);
            }

        // set credentials if given
        if ((useCredentials = getUsedProperty(properties, "useCredentials")) != null && useCredentials.matches("true")) {

            if ((dbuser = getUsedProperty(properties, "dbuser")) != null
                    && (dbpassword = getUsedProperty(properties, "dbpassword")) != null)
                clusterBuilder = clusterBuilder.withCredentials(dbuser, dbpassword);
            else {
                System.err.println("missing dbuser or dbpassword properties");
                System.exit(0);
            }

        }

        // build cluster
        Cluster cluster = clusterBuilder.build();
        return cluster;
    }

    /**
     * Retrive the properties by name.
     * 
     * @param properties
     *            - properties
     * @param name
     *            - name
     * @return
     */
    private static String getUsedProperty(Properties properties, String name) {
        String s = properties.getProperty(name);
        if (s != null && !s.isEmpty())
            return s;
        return null;
    }

    /**
     * Load properties.
     * 
     * @param properties
     *            - properties
     */
    private static void loadProperties(Properties properties) {
        String propertiesFilePath = System.getProperty("mutagenCassandra.properties.file");
        if (propertiesFilePath == null) {
            System.err.println("please provide VM argument \"mutagenCassandra.properties.file\"");
            System.exit(1);
        }
        InputStream input = null;
        try {

            input = new FileInputStream(propertiesFilePath);
            // load the properties file
            properties.load(input);
        } catch (Exception e) {
            throw new RuntimeException("Could not load properties!!!");
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {

                }
            }
        }
    }

    /**
     * Initializes the logging.
     *
     * @param level
     *            The minimum level to log at.
     */
    static void initLogging(Level level) {
        LogFactory.setLogCreator(new ConsoleLogCreator(level));
        LOG = LogFactory.getLog(Main.class);
    }

    /**
     * Checks the desired log level.
     *
     * @param args
     *            The command-line arguments.
     * @return The desired log level.
     */
    private static Level getLogLevel(String[] args) {
        for (String arg : args) {
            if ("-X".equals(arg)) {
                return Level.DEBUG;
            }
            if ("-q".equals(arg)) {
                return Level.WARN;
            }
        }
        return Level.INFO;
    }

    /**
     * Determine the operations that should be executed.
     *
     * @param args
     *            The command-line arguments passed in.
     * @return The operations. An empty list if none.
     */
    private static List<String> determineOperations(String[] args) {
        List<String> operations = new ArrayList<String>();

        for (String arg : args) {
            if (!arg.startsWith("-")) {
                operations.add(arg);
            }
        }

        return operations;
    }

    /**
     * Overrides the configuration from the config file with the properties passed in directly from the command-line.
     *
     * @param properties
     *            The properties to override.
     * @param args
     *            The command-line arguments that were passed in.
     */
    /* private -> for testing */
    static void overrideConfiguration(Properties properties, String[] args) {
        for (String arg : args) {
            if (isPropertyArgument(arg)) {
                properties.put(getArgumentProperty(arg), getArgumentValue(arg));
            }
        }
    }

    /**
     * Checks whether this command-line argument tries to set a property.
     *
     * @param arg
     *            The command-line argument to check.
     * @return {@code true} if it does, {@code false} if not.
     */
    /* private -> for testing */
    static boolean isPropertyArgument(String arg) {
        return arg.startsWith("-") && arg.contains("=");
    }

    /**
     * Retrieves the property this command-line argument tries to assign.
     *
     * @param arg
     *            The command-line argument to check, typically in the form -key=value.
     * @return The property.
     */
    /* private -> for testing */
    static String getArgumentProperty(String arg) {
        int index = arg.indexOf("=");

        return arg.substring(1, index);
    }

    /**
     * Retrieves the value this command-line argument tries to assign.
     *
     * @param arg
     *            The command-line argument to check, typically in the form -key=value.
     * @return The value or an empty string if no value is assigned.
     */
    /* private -> for testing */
    static String getArgumentValue(String arg) {
        int index = arg.indexOf("=");

        if ((index < 0) || (index == arg.length())) {
            return "";
        }

        return arg.substring(index + 1);
    }

    /**
     * Prints the usage instructions on the console.
     */
    private static void printUsage() {
        LOG.info("********");
        LOG.info("* Usage");
        LOG.info("********");
        LOG.info("");
        LOG.info("mutagen [options] command");
        LOG.info("");
        LOG.info("By default, the configuration will be read from conf/mutagen.conf.");
        LOG.info("Options passed from the command-line override the configuration.");
        LOG.info("");
        LOG.info("Commands");
        LOG.info("========");
        LOG.info("migrate  : Migrates the database");
        LOG.info("clean    : Drops all objects in the configured schemas");
        LOG.info("info     : Prints the information about applied, current and pending migrations");
        LOG.info("validate : Validates the applied migrations against the ones on the classpath");
        LOG.info("baseline : Baselines an existing database at the baselineVersion");
        LOG.info("repair   : Repairs the metadata table");
        LOG.info("");
        LOG.info("Options (Format: -key=value)");
        LOG.info("=======");
        LOG.info("baselineVersion        : Version to tag schema with when executing baseline");
        LOG.info("");
        LOG.info("Add -X to print debug output");
        LOG.info("Add -q to suppress all output, except for errors and warnings");
        LOG.info("");
        LOG.info("Example");
        LOG.info("=======");
        LOG.info("mutagen -baselineVersion=201412311234 baseline");
        LOG.info("");
    }
}
