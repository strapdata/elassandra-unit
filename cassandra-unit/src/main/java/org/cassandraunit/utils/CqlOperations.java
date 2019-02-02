package org.cassandraunit.utils;

import com.datastax.driver.core.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.function.Consumer;

public class CqlOperations {

    private static final Logger log = LoggerFactory.getLogger(CqlOperations.class);

    public static Consumer<String> execute(Session session) {
        return query -> {
            log.debug("executing : {}", query);
            session.execute(query);
        };
    }

    public static Consumer<String> use(Session session) {
        return keyspace -> session.execute("USE " + keyspace);
    }

    public static Consumer<String> truncateTable(Session session) {
        return fullyQualifiedTable -> session.execute("truncate table " + fullyQualifiedTable);
    }

    public static Consumer<String> dropKeyspace(Session session) {
        return keyspace -> execute(session).accept("DROP KEYSPACE IF EXISTS " + keyspace);
    }

    public static Consumer<String> createKeyspace(Session session) {
        return keyspace -> execute(session).accept(String.format(Locale.ROOT,
            "CREATE KEYSPACE IF NOT EXISTS %s WITH replication={'class' : 'NetworkTopologyStrategy', '%s':'1'} AND durable_writes = false",
            keyspace, session.getCluster().getMetadata().getAllHosts().iterator().next().getDatacenter()));
    }
}
