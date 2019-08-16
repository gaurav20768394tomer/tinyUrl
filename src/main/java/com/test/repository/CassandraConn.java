package com.test.repository;


import com.datastax.driver.core.*;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import javax.annotation.PostConstruct;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

@Repository
public class CassandraConn {

    private static final Logger logger = LoggerFactory.getLogger(CassandraConn.class);

    @Value("${cassandra.host}")
    private String hostList;

    @Value("${cassandra.cluster.name}")
    private String clusterName;

    @Value("${cassandra.cluster.username}")
    private String userName;

    @Value("${cassandra.cluster.password}")
    private String password;

    private static Cluster cluster;
    private static Session session;
    private static ConsistencyLevel consistencyLevel;

    @PostConstruct
    public void connectToCluster() throws Exception {
        runWithRetries(10, () -> {
            getCass();
        });

    }
    private void getCass() {
        try {
            PoolingOptions poolingOptions = new PoolingOptions();
            poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, 10);
            poolingOptions.setPoolTimeoutMillis(5000);
            poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, 10);
            PlainTextAuthProvider authProvider = new PlainTextAuthProvider(userName, password);
            //System.console().printf("hostList" + hostList);
            cluster = Cluster.builder().addContactPointsWithPorts(getHostIntetSocketAddressList(hostList))
                    /*.withClusterName(clusterName)*/.withAuthProvider(authProvider).withPoolingOptions(poolingOptions)
                    .build();

            consistencyLevel = getSession().getCluster().getMetadata().getAllHosts().size() > 1 ? ConsistencyLevel.ONE
                    : ConsistencyLevel.ONE;

        } catch (Exception ex) {
            ex.printStackTrace();
            getCass();
        }
    }

    private List<InetSocketAddress> getHostIntetSocketAddressList(String hostList) {
        List<InetSocketAddress> cassandraHosts = Lists.newArrayList();
        for (String host : hostList.split(",")) {
            InetSocketAddress socketAddress = new InetSocketAddress(host.split(":")[0],
                    Integer.valueOf(host.split(":")[1]));
            cassandraHosts.add(socketAddress);
        }
        return cassandraHosts;
    }

    public ConsistencyLevel getConsistencyLevel() {
        Optional<ConsistencyLevel> consistencyLevelOptional = Optional.of(consistencyLevel);
        if (consistencyLevelOptional.isPresent()) {
            return consistencyLevel;
        }
        return consistencyLevel;
    }

    public Session getSession() {
        if (cluster == null) {
            throw new RuntimeException("Cassandra Cluster in NULL");
        }
        if (session == null) {
            session = cluster.connect();
            String query1 = "CREATE KEYSPACE IF NOT EXISTS tinyurl WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}  AND durable_writes = true";

            String query2 = "USE tinyurl";

            String query3 = "CREATE TABLE IF NOT EXISTS urldata ("+
                    "id                          text, "+
                    "url                         text, "+
                    "PRIMARY KEY (id)) WITH " +
                    "bloom_filter_fp_chance=0.001000 AND "+
                    "caching={'keys': 'ALL' } AND "+
                    "comment='' AND "+
                    "gc_grace_seconds=10800 AND "+
                    "default_time_to_live=0 AND "+
                    "speculative_retry='99.0PERCENTILE' AND "+
                    "memtable_flush_period_in_ms=0 AND "+
                    "compaction={'class': 'LeveledCompactionStrategy'} AND "+
                    "compression={'sstable_compression': 'LZ4Compressor'}";

            session.execute(query1);
            session.execute(query2);
            session.execute(query3);
        }
        return session;
    }

    public void close() {
        if (session != null) {
            logger.debug("Closing Session....");
            try {
                session.close();
            } catch (Exception e) {
                logger.error("Error while closing the Session ...");
            }
        }
        if (cluster != null) {
            logger.debug("Closing Cluster....");
            try {
                cluster.close();
            } catch (Exception e) {
                logger.error("Error while closing the Cluster ...");
            }
        }
    }

    interface ThrowingTask {
        void run() throws ExecutionException;
    }
    boolean runWithRetries(int maxRetries, ThrowingTask t) {
        int count = 0;
        while (count < maxRetries) {
            try {
                logger.debug("trying........................\n\n\n" + count);
                t.run();
                return true;
            } catch (ExecutionException e) {
                if (++count >= maxRetries)
                    return false;
            }
        }
        return false;
    }

}
