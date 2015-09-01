package org.cassandraunit.utils;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

import java.util.Random;

import junit.framework.Assert;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

import org.junit.Ignore;
import org.junit.Test;

/**
 * UnitTest for EmbeddedCassandra with random port. Because Cassandra basically can only be started once per JVM, this test is
 * disabled, and should be manually enabled for single tests only. (CassandraDaemon#deactivate is a bad joke. There may be some
 * workaround with surefire-fork or classloaders or whatever, but one shouldnt invest too much in a workaround for a broken
 * external functionality)
 * 
 * @author Markus Kull
 */
@Ignore("Cassandra can only be started once. If you want to run this test, then enable it and run only this test")
public class EmbeddedCassandraServerHelperTest {

    @Test
    public void shouldStartupOnRandomFreePort() throws Exception {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra(EmbeddedCassandraServerHelper.CASSANDRA_RNDPORT_YML_FILE);
        int nativePort = EmbeddedCassandraServerHelper.getNativeTransportPort();
        int rpcPort = EmbeddedCassandraServerHelper.getRpcPort();
        Assert.assertTrue(nativePort > 0);
        Assert.assertTrue(rpcPort > 0);
        Assert.assertTrue(rpcPort != 9171); // may seldomly fail if system chooses exactly port 9171 ...
        testIfTheEmbeddedCassandraServerIsUpOnHost("127.0.0.1:" + rpcPort);
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
