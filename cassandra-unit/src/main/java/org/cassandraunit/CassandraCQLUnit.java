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

    private String hostIp = "127.0.0.1";
    private int port = 0;

    public Session session;
    public Cluster cluster;

    public CassandraCQLUnit(CQLDataSet dataSet) {
        this.dataSet = dataSet;
    }

    public CassandraCQLUnit(CQLDataSet dataSet, String configurationFileName) {
        this(dataSet);
        this.configurationFileName = configurationFileName;
    }

    // the following constructors with explicit hostip and port seem pretty useless because embedded cassandra
    // is started anyway on the default address

    public CassandraCQLUnit(CQLDataSet dataSet, String configurationFileName, String hostIp, int port) {
        this(dataSet);
        this.configurationFileName = configurationFileName;
        this.hostIp = hostIp;
        this.port = port;
    }

    public CassandraCQLUnit(CQLDataSet dataSet, String configurationFileName, String hostIp, int port, long startUpTimeout) {
        super(startUpTimeout);
        this.dataSet = dataSet;
        this.configurationFileName = configurationFileName;
        this.hostIp = hostIp;
        this.port = port;
    }

    @Override
    protected void load() {
        if (port == 0) {
            port = EmbeddedCassandraServerHelper.getNativeTransportPort();
        }

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
