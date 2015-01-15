package org.cassandraunit.dataset;

import static org.cassandraunit.SampleDataSetChecker.assertDataSetDefaultValues;

import org.cassandraunit.dataset.json.ClassPathJsonDataSet;
import org.cassandraunit.model.ColumnFamilyModel;
import org.cassandraunit.utils.FileTmpHelper;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * 
 * @author John Marshall 
 * 
 */
public class MultiSourceDataSetTest {

	private String targetXmlDataSetPathFileName = null;
	private String secondXmlDataSetPathFileName = null;
	private String alternateXmlDataSetPathFileName = null;
	private String targetJsonDataSetPathFileName = null;
	private String targetYamlDataSetPathFileName = null;

	@Before
	public void before() throws Exception {
		targetXmlDataSetPathFileName = FileTmpHelper.copyClassPathDataSetToTmpDirectory(this.getClass(),
				"/xml/dataSetDefaultValues.xml");
        secondXmlDataSetPathFileName = FileTmpHelper.copyClassPathDataSetToTmpDirectory(this.getClass(),
                "/xml/dataSetDefaultValuesMultiple.xml");
        alternateXmlDataSetPathFileName = FileTmpHelper.copyClassPathDataSetToTmpDirectory(this.getClass(),
                "/xml/dataSetDefinedValues.xml");
		targetJsonDataSetPathFileName = FileTmpHelper.copyClassPathDataSetToTmpDirectory(this.getClass(),
				"/json/dataSetDefaultValues.json");
		targetYamlDataSetPathFileName = FileTmpHelper.copyClassPathDataSetToTmpDirectory(this.getClass(),
				"/yaml/dataSetDefaultValues.yaml");
	}

    // Test reading from single files

	@Test
	public void shouldGetAJsonDataSetStructureFromASingleFile() throws Exception {
		DataSet dataSet = MultiSourceDataSet.fromFiles(targetJsonDataSetPathFileName);
		assertDataSetDefaultValues(dataSet);
	}

	@Test(expected = ParseException.class)
	public void shouldNotGetADataSetStructureBecauseOfNull() {
		DataSet dataSet = MultiSourceDataSet.fromFiles(null);
		dataSet.getKeyspace();
	}

	@Test(expected = ParseException.class)
	public void shouldNotGetAJsonDataSetStructureBecauseOfFileNotFound() {
		DataSet dataSet = MultiSourceDataSet.fromFiles("/notfound.json");
		dataSet.getKeyspace();
	}

	@Test
	public void shouldGetAXmlDataSetStructure() throws Exception {

		DataSet dataSet = MultiSourceDataSet.fromFiles(targetXmlDataSetPathFileName);
		assertDataSetDefaultValues(dataSet);
	}

	@Test(expected = ParseException.class)
	public void shouldNotGetAXmlDataSetStructureBecauseOfFileNotFound() {
		DataSet dataSet = MultiSourceDataSet.fromFiles("/notfound.xml");
		dataSet.getKeyspace();
	}

	@Test
	public void shouldGetAYamlDataSetStructure() throws Exception {

		DataSet dataSet = MultiSourceDataSet.fromFiles(targetYamlDataSetPathFileName);
		assertDataSetDefaultValues(dataSet);
	}

	@Test(expected = ParseException.class)
	public void shouldNotGetAYamlDataSetStructureBecauseOfFileNotFound() {
		DataSet dataSet = MultiSourceDataSet.fromFiles("/notfound.yaml");
		dataSet.getKeyspace();
	}

    // Test reading from classpath

    @Test
    public void shouldGetAJsonDataSetStructureFromASingleClasspathFile() {
        DataSet dataSet = MultiSourceDataSet.fromClassPath("json/dataSetDefaultValues.json");
        assertDataSetDefaultValues(dataSet);
    }

    @Test(expected = ParseException.class)
    public void shouldNotGetADataSetStructureFromClasspathBecauseOfNull() {
        DataSet dataSet = MultiSourceDataSet.fromClassPath(null);
        dataSet.getKeyspace();
    }

    @Test(expected = ParseException.class)
    public void shouldNotGetAJsonDataSetStructureFromClasspathBecauseOfDataSetNotExist() {
        DataSet dataSet = MultiSourceDataSet.fromClassPath("json/unknown.json");
        dataSet.getKeyspace();
    }

    @Test
    public void shouldGetAXmlDataSetStructureFromClasspath() {
        DataSet dataSet = MultiSourceDataSet.fromClassPath("xml/dataSetDefaultValues.xml");
        assertDataSetDefaultValues(dataSet);
    }

    @Test(expected = ParseException.class)
    public void shouldNotGetAXmlDataSetStructureFromClasspathBecauseOfDataSetNotExist() {
        DataSet dataSet = MultiSourceDataSet.fromClassPath("xml/unknown.xml");
        dataSet.getKeyspace();
    }

    @Test
    public void shouldGetAYamlDataSetStructureFromClasspath() {
        DataSet dataSet = MultiSourceDataSet.fromClassPath("yaml/dataSetDefaultValues.yaml");
        assertDataSetDefaultValues(dataSet);
    }

    @Test(expected = ParseException.class)
    public void shouldNotGetAYamlDataSetStructureFromClasspathBecauseOfDataSetNotExist() {
        DataSet dataSet = MultiSourceDataSet.fromClassPath("yaml/unknown.yaml");
        dataSet.getKeyspace();
    }

    // Test the main functionality - merging multiple files - negative cases

    private static void assertThrowsParseExceptionIncludingMessage(String message, String ... filenames) {
        try {
            MultiSourceDataSet.fromFiles(filenames);
            fail("Expected a ParseException with message '" + message + "' to be thrown");
        } catch(ParseException pe) {
            assertTrue(pe.getMessage().contains(message));
        } catch (Exception e) {
            fail("Expected a ParseException with message '" + message + "' to be thrown");
        }
    }


    @Test
    public void shouldNotBeAbleToMergeMultipleFilesIfKeyspacesDiffer() {
        assertThrowsParseExceptionIncludingMessage(
            "Only one keyspace name is supported",
            targetXmlDataSetPathFileName, alternateXmlDataSetPathFileName);
    }

    @Test
    public void shouldNotBeAbleToMergeTheSameFileTwice() {
        assertThrowsParseExceptionIncludingMessage(
            "Duplicate Column Family name(s) found while checking whether datasets can be merged",
            targetXmlDataSetPathFileName, targetXmlDataSetPathFileName);
    }

    // Test the main functionality - merging multiple files - positive cases

    @Test
    public void shouldBeAbleToMergeMultipleFilesOfSameFileFormat() {
        DataSet dataSet = MultiSourceDataSet.fromFiles(targetXmlDataSetPathFileName, secondXmlDataSetPathFileName);
        List<ColumnFamilyModel> columnFamilies = dataSet.getColumnFamilies();

        assertNotNull(columnFamilies);
        assertEquals(2, columnFamilies.size());
        assertEquals("columnFamily1", columnFamilies.get(0).getName());
        assertEquals("columnFamily2", columnFamilies.get(1).getName());
    }

    @Test
    public void shouldBeAbleToMergeMultipleFilesOfDifferentFileFormat() {
        DataSet dataSet = MultiSourceDataSet.fromFiles(targetJsonDataSetPathFileName, secondXmlDataSetPathFileName);
        List<ColumnFamilyModel> columnFamilies = dataSet.getColumnFamilies();

        assertNotNull(columnFamilies);
        assertEquals(2, columnFamilies.size());
        assertEquals("columnFamily1", columnFamilies.get(0).getName());
        assertEquals("columnFamily2", columnFamilies.get(1).getName());
    }
}
