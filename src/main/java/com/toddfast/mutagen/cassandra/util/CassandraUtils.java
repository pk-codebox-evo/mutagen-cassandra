package com.toddfast.mutagen.cassandra.util;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;

import java.util.List;

/**
 * @author Manuel Boillod
 */
public class CassandraUtils {

    /**
     * Create cluster.
     **/
    public static Cluster createCluster(List<String> clusterContactPoints) {
        return createCluster(clusterContactPoints, null, false, null, null);
    }

    /**
     * Create cluster.
     **/
    public static Cluster createCluster(List<String> clusterContactPoints,
                                         String clusterPort,
                                         boolean useCredentials,
                                         String dbuser,
                                         String dbpassword) {

        // get cluster builder
        Cluster.Builder clusterBuilder = Cluster.builder().withProtocolVersion(ProtocolVersion.V2);

        // set contact point
        if (clusterContactPoints != null) {
            for (String clusterContactPoint : clusterContactPoints) {
                clusterBuilder.addContactPoint(clusterContactPoint);
            }
        }

        // set cluster port if given
        if (clusterPort != null) {
            try {
                clusterBuilder.withPort(Integer.parseInt(clusterPort));
            } catch (NumberFormatException e) {
                System.err.println("Port parameter must be an integer");
                System.exit(1);
            }
        }

        // set credentials if given
        if (useCredentials) {
            if (dbuser != null && dbpassword != null) {
                clusterBuilder = clusterBuilder.withCredentials(dbuser, dbpassword);
            } else {
                System.err.println("missing dbuser or dbpassword properties");
                System.exit(0);
            }

        }

        // build cluster
        return clusterBuilder.build();
    }

    public static Session getSession(Cluster cluster, String keyspace) {
        return (keyspace != null ?
                cluster.connect(keyspace) :
                cluster.connect());
    }

}
