package org.cassandraunit;

import me.prettyprint.cassandra.model.CqlQuery;
import me.prettyprint.cassandra.serializers.StringSerializer;

import org.cassandraunit.dataset.json.ClassPathJsonDataSet;
import org.junit.Rule;
import org.junit.Test;

public class DataLoaderAndCQLExecutionTest {

	@Rule
	public CassandraUnit cassandraUnit = new CassandraUnit(new ClassPathJsonDataSet("json/dataSetCqlDataSet.json"));

	@Test
	public void doQuery() {
		final CqlQuery<String, String, String> query = new CqlQuery<String, String, String>(cassandraUnit.keyspace,
				StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
		query.setCqlVersion("3");
		query.setQuery("SELECT * FROM test WHERE KEY='KEY'");
		query.execute();
	}

}
