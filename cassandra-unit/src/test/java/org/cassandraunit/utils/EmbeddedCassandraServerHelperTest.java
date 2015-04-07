package org.cassandraunit.utils;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

import java.util.Random;

import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

import org.junit.Test;

/**
 * 
 * @author Jeremy Sevellec
 * 
 */
public class EmbeddedCassandraServerHelperTest {

    @Test
    public void shouldStartAndCleanAnEmbeddedCassandra() throws Exception {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra();
        testIfTheEmbeddedCassandraServerIsUpOnHost("127.0.0.1:9171");
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
        EmbeddedCassandraServerHelper.startEmbeddedCassandra();
        testIfTheEmbeddedCassandraServerIsUpOnHost("127.0.0.1:9171");
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
    }

    private void testIfTheEmbeddedCassandraServerIsUpOnHost(String hostAndPort) {
        Random random = new Random();
        Cluster cluster = HFactory.getOrCreateCluster("TestCluster" + random.nextInt(), new CassandraHostConfigurator(
                hostAndPort));
        try {
            assertThat(cluster.getConnectionManager().getActivePools().size(), is(1));
            KeyspaceDefinition keyspaceDefinition = cluster.describeKeyspace("system");
            assertThat(keyspaceDefinition, notNullValue());
            assertThat(keyspaceDefinition.getReplicationFactor(), is(1));
        } finally {
            // due to random the created cluster cannot be used ever anyway
            HFactory.shutdownCluster(cluster);
        }
    }
}
