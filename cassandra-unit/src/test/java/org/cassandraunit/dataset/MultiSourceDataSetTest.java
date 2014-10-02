package org.cassandraunit.dataset;

import static org.cassandraunit.SampleDataSetChecker.assertDataSetDefaultValues;

import com.sun.corba.se.impl.orb.ParserTable;
import org.cassandraunit.dataset.json.ClassPathJsonDataSet;
import org.cassandraunit.utils.FileTmpHelper;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * @author John Marshall 
 * 
 */
public class MultiSourceDataSetTest {

	private String targetXmlDataSetPathFileName = null;
	private String alternateXmlDataSetPathFileName = null;
	private String targetJsonDataSetPathFileName = null;
	private String targetYamlDataSetPathFileName = null;

	@Before
	public void before() throws Exception {
		targetXmlDataSetPathFileName = FileTmpHelper.copyClassPathDataSetToTmpDirectory(this.getClass(),
				"/xml/dataSetDefaultValues.xml");
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

    // Test the main functionality - merging multiple files

    @Test(expected = ParseException.class)
    public void shouldNotBeAbleToMergeMultipleFilesIfKeyspacesDiffer() {
        MultiSourceDataSet.fromFiles(targetXmlDataSetPathFileName, alternateXmlDataSetPathFileName);
    }

    @Test(expected = ParseException.class)
    public void shouldNotBeAbleToMergeTheSameFileTwice() {
        MultiSourceDataSet.fromFiles(targetXmlDataSetPathFileName, targetXmlDataSetPathFileName);
    }

    @Test(expected = ParseException.class)
    public void shouldNotBeAbleToGetColumnFamiliesIfDuplicateNamesExist() {
        MultiSourceDataSet.fromFiles(targetXmlDataSetPathFileName, alternateXmlDataSetPathFileName);
    }
}
