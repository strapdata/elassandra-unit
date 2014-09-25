package org.cassandraunit.dataset;


import org.cassandraunit.model.ColumnFamilyModel;
import org.cassandraunit.model.KeyspaceModel;

import java.util.ArrayList;
import java.util.List;

/**
 * Allowing multiple files/classpath-resources containing DataSets
 * to be specified,
 * as long as they all belong to the same
 * overall keyspace. This class just merges all of the column-families together.
 */
public class MultiSourceDataSet implements DataSet {

    private final List<DataSet> dataSets;

    public static MultiSourceDataSet fromClassPath(String... classpathFileNames) {
        List<DataSet> ds = new ArrayList<DataSet>(classpathFileNames.length);
        for (String fileName : classpathFileNames) {
            ds.add(new ClassPathDataSet(fileName));
        }
        return new MultiSourceDataSet(ds);
    }

    public static MultiSourceDataSet fromFiles(String... fileNames) {
        List<DataSet> ds = new ArrayList<DataSet>(fileNames.length);
        for (String fileName : fileNames) {
            ds.add(new FileDataSet(fileName));
        }
        return new MultiSourceDataSet(ds);
    }

    private MultiSourceDataSet(List<DataSet> dataSets) {
        this.dataSets = dataSets;
        String keyspaceName = "";

        for (DataSet dataSet : dataSets) {
            if (keyspaceName.isEmpty()) {
                keyspaceName = dataSet.getKeyspace().getName();
            } else {
                if (!keyspaceName.equals(dataSet.getKeyspace().getName())) {
                    throw new ParseException(
                        new IllegalArgumentException("Only one keyspace name is supported:" +
                            "was expecting " + keyspaceName + " but found " + dataSet.getKeyspace().getName()));
                }
            }
        }
    }

    private DataSet firstDataSet() {
       return dataSets.get(0);
    }

    @Override
    public KeyspaceModel getKeyspace() {
        return firstDataSet().getKeyspace();
    }

    @Override
    public List<ColumnFamilyModel> getColumnFamilies() {
        if (dataSets.size() == 1) {
            return firstDataSet().getColumnFamilies();
        }

        List<ColumnFamilyModel> mergedColumnFamilies = new ArrayList<ColumnFamilyModel>();

        for (DataSet dataSet : dataSets) {
            mergedColumnFamilies.addAll(dataSet.getColumnFamilies());
        }

        return mergedColumnFamilies;
    }
}