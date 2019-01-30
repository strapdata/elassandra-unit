package org.cassandraunit.dataset.cql;

import org.cassandraunit.dataset.CQLDataSet;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

/**
 * @author Jeremy Sevellec
 */
public class SimpleCQLDataSet extends AbstractCQLDataSet implements CQLDataSet {

    public SimpleCQLDataSet(String cqlStatements) {
        super(cqlStatements, true, true, null);
    }

    public SimpleCQLDataSet(String cqlStatements, boolean keyspaceCreation, boolean keyspaceDeletion) {
        super(cqlStatements, keyspaceCreation, keyspaceDeletion, null);
    }

    public SimpleCQLDataSet(String cqlStatements, String keyspaceName) {
        super(cqlStatements, true, true, keyspaceName);
    }

    public SimpleCQLDataSet(String cqlStatements, boolean keyspaceCreation) {
      super(cqlStatements, keyspaceCreation, true, null);
    }

    public SimpleCQLDataSet(String cqlStatements, boolean keyspaceCreation, boolean keyspaceDeletion, String keyspaceName) {
        super(cqlStatements, keyspaceCreation, keyspaceDeletion, keyspaceName);
    }

    @Override
    protected InputStream getInputDataSetLocation(String cqlStatements) {
        return new ByteArrayInputStream(cqlStatements.getBytes());
    }
}
