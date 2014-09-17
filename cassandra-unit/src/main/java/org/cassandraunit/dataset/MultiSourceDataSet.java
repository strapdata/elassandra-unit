package org.cassandraunit.dataset;


import org.cassandraunit.dataset.commons.ParsedColumnFamily;
import org.cassandraunit.dataset.commons.ParsedKeyspace;
import org.cassandraunit.dataset.json.AbstractJsonDataSet;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Decorates an {@link org.cassandraunit.dataset.json.AbstractJsonDataSet},
 * allowing multiple seed files to be specified,
 * as long as they all belong to the same
 * overall keyspace. This class just merges all of the column-families together.
 *
 *
 *
 */
public class MultiSourceDataSet implements DataSet {

    private final AbstractJsonDataSet delegate;
    private final List<String> filenames;

    public static MultiSourceDataSet fromClassPath(String... classpathFileNames) {

    }

    public static MultiSourceDataSet fromFiles(String... fileNames) {
        for (String fileName : fileNames) {

        }
    }

    public MultiSourceDataSet(AbstractJsonDataSet delegate, List<String> filenames) {
        //super(filenames.get(0));
        this.filenames = filenames;
    }

    protected ParsedKeyspace getParsedKeyspace() {

        Set<ParsedKeyspace> parsedKeyspaces = new HashSet<ParsedKeyspace>();
        String keyspaceName = "";

        for (String dataSetFilename : filenames) {
            InputStream inputDataSetLocation = delegate.getInputDataSetLocation(dataSetFilename);
            if (inputDataSetLocation == null) {
                throw new ParseException("Dataset not found");
            }

            ObjectMapper jsonMapper = new ObjectMapper();
            try {
                ParsedKeyspace pk = jsonMapper.readValue(inputDataSetLocation, ParsedKeyspace.class);

                if (!keyspaceName.isEmpty() && keyspaceName.equals(pk.getName())) {
                    throw new ParseException(
                            new IllegalArgumentException("Only one keyspace name is supported:" +
                                    "was expecting " + keyspaceName + " but found " + pk.getName()));
                }

                parsedKeyspaces.add(pk);
            } catch (JsonParseException e) {
                throw new ParseException(e);
            } catch (JsonMappingException e) {
                throw new ParseException(e);
            } catch (IOException e) {
                throw new ParseException(e);
            }
        }

        return mergeParsedKeyspaces(parsedKeyspaces);
    }

    private ParsedKeyspace mergeParsedKeyspaces(Set<ParsedKeyspace> pks) {
        if (pks.isEmpty()) {
            throw new ParseException(
                    new IllegalStateException("No keyspaces were found. Can't merge anything."));
        }

        ParsedKeyspace firstKeyspace = pks.iterator().next();

        if (pks.size() == 1) {
            return firstKeyspace;
        }

        List<ParsedColumnFamily> mergedColumnFamilies = new ArrayList<ParsedColumnFamily>();

        for (ParsedKeyspace pk : pks) {
            mergedColumnFamilies.addAll(pk.getColumnFamilies());
        }

        firstKeyspace.setColumnFamilies(mergedColumnFamilies);
        return firstKeyspace;
    }
}