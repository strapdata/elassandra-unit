package org.cassandraunit;

import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.rules.ExternalResource;

/**
 * @author Marcin Szymaniuk
 */
public abstract class BaseCassandraUnit extends ExternalResource {
    
    protected String configurationFileName;
    protected long startupTimeout;

    public BaseCassandraUnit() {
        this(EmbeddedCassandraServerHelper.DEFAULT_STARTUP_TIMEOUT);
    }
    
    public BaseCassandraUnit(long startupTimeout) {
        this.startupTimeout = startupTimeout;
    }

    @Override
    protected void before() throws Exception {
        /* start an embedded Cassandra */
        if (configurationFileName != null) {
            EmbeddedCassandraServerHelper.startEmbeddedCassandra(configurationFileName, startupTimeout);
        } else {
            EmbeddedCassandraServerHelper.startEmbeddedCassandra(startupTimeout);
        }

        /* create structure and load data */
        load();
    }

    protected abstract void load();
}
