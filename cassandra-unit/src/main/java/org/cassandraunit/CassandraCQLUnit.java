package org.cassandraunit;

import org.cassandraunit.dataset.CQLDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
/**
 * @author Marcin Szymaniuk
 * @author Jeremy Sevellec
 */
public class CassandraCQLUnit extends BaseCassandraUnit {
    private CQLDataSet dataSet;

    public Session session;
    public Cluster cluster;

    public CassandraCQLUnit(CQLDataSet dataSet) {
        this.dataSet = dataSet;
    }

    public CassandraCQLUnit(CQLDataSet dataSet, String configurationFileName) {
        this(dataSet);
        this.configurationFileName = configurationFileName;
    }

    // The former constructors with hostip and port have been removed. Now host+port is directly read out of the provided
    // configurationFile(Name). You may also use EmbeddedCassandraServerHelper.CASSANDRA_RNDPORT_YML_FILE to select 
    // random (free) ports for EmbeddedCassandra, such that you can start multiple embedded cassandras on the same host
    // (but not in the same JVM).

    public CassandraCQLUnit(CQLDataSet dataSet, String configurationFileName, long startUpTimeout) {
        super(startUpTimeout);
        this.dataSet = dataSet;
        this.configurationFileName = configurationFileName;
    }

    @Override
    protected void load() {
        String hostIp = EmbeddedCassandraServerHelper.getHost();
        int port = EmbeddedCassandraServerHelper.getNativeTransportPort();
        cluster = new Cluster.Builder().addContactPoints(hostIp).withPort(port).build();
        session = cluster.connect();
        CQLDataLoader dataLoader = new CQLDataLoader(session);
        dataLoader.load(dataSet);
        session = dataLoader.getSession();
    }

    @Override
    protected void after() {
        super.after();
        try (Cluster c = cluster; Session s = session) {
            session = null;
            cluster = null;
        }
    }

    // Getters for those who do not like to directly access fields

    public Session getSession() {
        return session;
    }

    public Cluster getCluster() {
        return cluster;
    }
}
