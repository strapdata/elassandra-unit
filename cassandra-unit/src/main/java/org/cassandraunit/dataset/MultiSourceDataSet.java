package org.cassandraunit.dataset;


import org.cassandraunit.model.ColumnFamilyModel;
import org.cassandraunit.model.KeyspaceModel;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Allowing multiple files/classpath-resources containing DataSets
 * to be specified, as long as they all belong to the same
 * overall keyspace, and don't have clashing column families.
 *
 * The specified files can be any mixture of JSON, XML or YAML.
 *
 * This class just merges all of the column-families together.
 */
public class MultiSourceDataSet implements DataSet {

    private final List<DataSet> dataSets = new ArrayList<DataSet>();
    private final List<ColumnFamilyModel> mergedColumnFamilies = new ArrayList<ColumnFamilyModel>();

    public static MultiSourceDataSet fromClassPath(String... classpathFileNames) {
        List<DataSet> ds = buildDataSetList(classpathFileNames);
        for (String fileName : classpathFileNames) {
            ds.add(new ClassPathDataSet(fileName));
        }
        return new MultiSourceDataSet(ds);
    }

    public static MultiSourceDataSet fromFiles(String... fileNames) {
        List<DataSet> ds = buildDataSetList(fileNames);
        for (String fileName : fileNames) {
            ds.add(new FileDataSet(fileName));
        }
        return new MultiSourceDataSet(ds);
    }

    private static List<DataSet> buildDataSetList(String... fileNames) {
        if (fileNames == null) {
            throw new ParseException("A non-null list of filenames must be supplied");
        }
        return new ArrayList<DataSet>(fileNames.length);
    }

    private MultiSourceDataSet(List<DataSet> dataSets) {
        String keyspaceName = "";

        for (DataSet dataSet : dataSets) {
            if (keyspaceName.isEmpty()) {
                keyspaceName = dataSet.getKeyspace().getName();
            } else {
                checkForInconsistentKeyspaceNames(keyspaceName, dataSet);
            }
            checkForDuplicateColumnFamilies(dataSet);
            mergedColumnFamilies.addAll(dataSet.getColumnFamilies());
            this.dataSets.add(dataSet);
        }
    }

    private final void checkForInconsistentKeyspaceNames(String keyspaceName, DataSet dataSet) {
        if (!keyspaceName.equals(dataSet.getKeyspace().getName())) {
            throw new ParseException(
                    new IllegalArgumentException("Only one keyspace name is supported: " +
                            "was expecting " + keyspaceName + " but found " + dataSet.getKeyspace().getName()));
        }
    }


    private final void checkForDuplicateColumnFamilies(DataSet mergeCandidate) {
        Set<String> intersection = getColumnFamilyNames(mergedColumnFamilies);
        Set<String> candidateColumnFamilyNames = getColumnFamilyNames(mergeCandidate.getColumnFamilies());
        intersection.retainAll(candidateColumnFamilyNames);
        if (!intersection.isEmpty()) {
            throw new ParseException("Duplicate Column Family name(s) found " +
                    "while checking whether datasets can be merged:" + intersection);
        }
    }

    private static final Set<String> getColumnFamilyNames(List<ColumnFamilyModel> columnFamilies) {
        Set<String> names = new HashSet<String>();
        for (ColumnFamilyModel cfm : columnFamilies) {
            names.add(cfm.getName());
        }
        return names;
    }

    @Override
    public KeyspaceModel getKeyspace() {
        return dataSets.get(0).getKeyspace();
    }

    @Override
    public List<ColumnFamilyModel> getColumnFamilies() {
        return mergedColumnFamilies;
    }
}