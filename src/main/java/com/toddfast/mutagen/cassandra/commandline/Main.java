package com.toddfast.mutagen.cassandra.commandline;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.toddfast.mutagen.MutagenException;
import com.toddfast.mutagen.Mutation;
import com.toddfast.mutagen.cassandra.util.CassandraUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.toddfast.mutagen.Plan;
import com.toddfast.mutagen.Plan.Result;
import com.toddfast.mutagen.cassandra.CassandraMutagen;
import com.toddfast.mutagen.cassandra.impl.CassandraMutagenImpl;
import com.toddfast.mutagen.cassandra.impl.info.MigrationInfoCommand;

public class Main {
    private static Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        initLogging(args);
        // detect operations
        List<String> operations = determineOperations(args);
        if (operations.isEmpty()) {
            printUsage();
            return;
        }
        // init properties
        Properties properties = new Properties();
        loadProperties(properties);
        overrideConfiguration(properties, args);
        loadLocationToClasspath(properties);

        try (Cluster cluster = createCluster(properties);
                Session session = createSession(cluster, properties)) {
            CassandraMutagen mutagen = new CassandraMutagenImpl(session);
            mutagen.configure(properties);
            for (String operation : operations) {
                LOGGER.info("execute operation '" + operation+"'");
                executeOperation(mutagen, operation);
            }
        } catch (Exception e) {
            LOGGER.error("Unexpected error", e);
            System.exit(1);
        }
    }

    private static void executeOperation(CassandraMutagen mutagen, String operation) {
        if ("clean".equals(operation)) {
            mutagen.clean();
        } else if ("baseline".equals(operation)) {
            loadResources(mutagen);
            mutagen.baseline();
        } else if ("migrate".equals(operation)) {
            String location = setLocation(mutagen);
            Result<String> mutationResult = mutagen.migrate(location);

            showMigrationResults(mutationResult);
            MutagenException mutationResultException = mutationResult.getException();
            if (mutationResultException != null) {
                throw mutationResultException;
            }
        } else if ("info".equals(operation)) {
            mutagen.info().refresh();
            LOGGER.info("\n" + MigrationInfoCommand.printInfo(mutagen.info().all()));
        } else if ("repair".equals(operation)) {
            mutagen.repair();
        } else {
            LOGGER.error("Invalid operation: " + operation);
            printUsage();
            System.exit(1);
        }

    }

    /**
     * Add the location in the classpath.
     */
    public static void loadLocationToClasspath(Properties properties) {
        String location = properties.getProperty("location");
        if (location == null || !location.contains("/")) {
            addToClassPath(".");
            return;
        }

        if (location.lastIndexOf("/") == location.length() - 1) {
            location = location.substring(0, location.length() - 1);
        }
        if (location.lastIndexOf("/") > 0) {
            location = location.substring(0, location.lastIndexOf("/"));
        }

        addToClassPath(location);
    }

    /**
     * Check if the path is in the classpath.
     * 
     * @param path
     *            - path.
     * @return boolean.
     */
    public static boolean isInClassPath(String path) {
        URLClassLoader urlClassLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();
        URL[] urls = urlClassLoader.getURLs();
        for (URL url : urls) {
            if (path.equals(url.toString()))
                return true;
        }
        return false;
    }

    /**
     * Add the resources under the classpath.
     * 
     * @param path
     *            - the resource path.
     */
    public static void addToClassPath(String path) {
        if (isInClassPath(path))
            return;

        try {
            URL url = new File(path).toURI().toURL();
            URLClassLoader urlClassLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();
            Class<URLClassLoader> urlClass = URLClassLoader.class;
            Method method = urlClass.getDeclaredMethod("addURL", URL.class);
            method.setAccessible(true);
            method.invoke(urlClassLoader, url);
        } catch (Exception e) {
            throw new RuntimeException("Can not load " + path, e);
        }
    }

    /**
     * Load resources.
     */
    private static void loadResources(CassandraMutagen mutagen) {
        try {
            setLocation(mutagen);
            mutagen.initialize();
        } catch (IOException e) {
            LOGGER.error("Can not load resources");
            throw new RuntimeException("Can not load resources");
        }
    }

    /**
     */
    private static String setLocation(CassandraMutagen mutagen) {
        String location = mutagen.getLocation();
        if (location == null) {
            throw new IllegalArgumentException("no location");
        }
        if (location.lastIndexOf("/") == location.length() - 1) {
            location = location.substring(0, location.length() - 1);
        }
        if (location.lastIndexOf("/") > 0) {
            location = location.substring(location.lastIndexOf("/") + 1, location.length());
        }

        mutagen.setLocation(location);
        return location;
    }

    /**
     * Show the result of migrations.
     * 
     * @param result
     *            - the result of migration.
     */
    private static void showMigrationResults(Plan.Result<String> result) {
        printMutations("Completed mutations:", result.getCompletedMutations());
        printMutations("Remaining mutations:", result.getRemainingMutations());
        if (result.isMutationComplete()) {
            LOGGER.info("Migration is finished.");
        } else {
            LOGGER.error("Migration aborted!");
        }
    }

    protected static void printMutations(String title, List<Mutation<String>> mutations) {
        LOGGER.info(title + Collections2.transform(mutations, new Function<Mutation<?>, String>() {
            @Override
            public String apply(Mutation<?> mutation) {
                return "\n\t- " + mutation;
            }
        }));
    }

    /**
     * Create session.
     *
     * @param properties
     *            - properties
     */
    private static Session createSession(Cluster cluster, Properties properties) {

        String keyspace = getProperty(properties, "keyspace");
        return CassandraUtils.getSession(cluster, keyspace);

    }

    /**
     * Create cluster.
     *
     * @param properties
     *            - properties.
     */
    private static Cluster createCluster(Properties properties) {
        String clusterContactPointsValue = getProperty(properties, "clusterContactPoints");
        List<String> clusterContactPoints = null;
        if (clusterContactPointsValue != null) {
            clusterContactPoints = Arrays.asList(clusterContactPointsValue.split(","));
        }

        String clusterPort = getProperty(properties, "clusterPort");

        String useCredentialsValue = getProperty(properties, "useCredentials");
        boolean useCredentials = useCredentialsValue != null &&
                useCredentialsValue.equals("true");

        String dbuser = getProperty(properties, "dbuser");

        String dbpassword = getProperty(properties, "dbpassword");

        return CassandraUtils.createCluster(clusterContactPoints, clusterPort, useCredentials, dbuser, dbpassword);
    }

    /**
     * Retrive the properties by name.
     * 
     * @param properties
     *            - properties
     * @param name
     *            - name
     */
    private static String getProperty(Properties properties, String name) {
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
            LOGGER.error("please provide VM argument \"mutagenCassandra.properties.file\"");
            System.exit(1);
        }
        try (InputStream input = new FileInputStream(propertiesFilePath)) {
            // load the properties file
            properties.load(input);
        } catch (Exception e) {
            throw new RuntimeException("Could not load properties file " + propertiesFilePath, e);
        }
    }

    /**
     * Initializes the logging.
     */
    public static void initLogging(String[] args) {
        for (String arg : args) {
            if ("-I".equals(arg)) {
                initLogging(Level.INFO);
            }
            if ("-X".equals(arg)) {
                initLogging(Level.DEBUG);
            }
            if ("-q".equals(arg)) {
                initLogging(Level.WARN);
            }
        }
    }

    public static void initLogging(Level level) {
        ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) LoggerFactory
                .getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);

        root.setLevel(level);
    }

    /**
     * Determine the operations that should be executed.
     *
     * @param args
     *            The command-line arguments passed in.
     * @return The operations. An empty list if none.
     */
    private static List<String> determineOperations(String[] args) {
        List<String> operations = new ArrayList<>();

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
        LOGGER.info("********");
        LOGGER.info("* Usage");
        LOGGER.info("********");
        LOGGER.info("");
        LOGGER.info("mutagen [options] command");
        LOGGER.info("");
        LOGGER.info("By default, the configuration will be read from conf/mutagen.conf.");
        LOGGER.info("Options passed from the command-line override the configuration.");
        LOGGER.info("");
        LOGGER.info("Commands");
        LOGGER.info("========");
        LOGGER.info("migrate  : Migrates the database");
        LOGGER.info("clean    : Drops all objects in the configured schemas");
        LOGGER.info("info     : Prints the information about applied, current and pending migrations");
        LOGGER.info("baseline : Baselines an existing database at the baselineVersion");
        LOGGER.info("repair   : Repairs the metadata table");
        LOGGER.info("");
        LOGGER.info("Options (Format: -key=value)");
        LOGGER.info("=======");
        LOGGER.info("baselineVersion        : Version to tag schema with when executing baseline");
        LOGGER.info("location               : Classpath locations to sacn recursively for migrations");
        LOGGER.info("");
        LOGGER.info("Add -I to print info output");
        LOGGER.info("Add -X to print debug output");
        LOGGER.info("Add -q to suppress all output, except for errors and warnings");
        LOGGER.info("");
        LOGGER.info("Example");
        LOGGER.info("=======");
        LOGGER.info("mutagen -baselineVersion=201412311234 baseline");
        LOGGER.info("mutagen -location=/path/mutations migrate");
        LOGGER.info("");
    }
}
