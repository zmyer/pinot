package com.linkedin.thirdeye.datalayer;

import com.linkedin.thirdeye.datalayer.dao.AnomalyFunctionDAO;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;
import java.io.File;
import java.io.FileReader;
import java.net.URL;
import java.sql.Connection;
import javax.sql.DataSource;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

public class AbstractBaseTest {
  ScriptRunner scriptRunner;
  Connection conn;
  protected AnomalyFunctionDAO anomalyFunctionDAO;

  @BeforeClass(alwaysRun = true)
  public void init () throws Exception {
    URL configUrl = getClass().getResource("/persistence-local.yml");
    File configFile = new File(configUrl.toURI());
    DaoProviderUtil.initConfiguration(configFile);
    DataSource ds = DaoProviderUtil.getDataSource();
    conn = ds.getConnection();
    // create schama
    URL createSchemaUrl = getClass().getResource("/schema/create-schema.sql");
    scriptRunner = new ScriptRunner(conn, false, false);
    scriptRunner.setDelimiter(";", true);
    scriptRunner.runScript(new FileReader(createSchemaUrl.getFile()));
    
    DaoProviderUtil.initGuiceInjector();
    anomalyFunctionDAO = DaoProviderUtil.getInstance(AnomalyFunctionDAO.class);
  }
  @AfterClass(alwaysRun = true)
  public void cleanUp () throws Exception {
    URL deleteSchemaUrl = getClass().getResource("/schema/drop-tables.sql");
    scriptRunner.runScript(new FileReader(deleteSchemaUrl.getFile()));
  }

  public static void main (String [] args) throws Exception {
    AbstractBaseTest baseTest = new AbstractBaseTest();
    baseTest.init();

    // this drops the tables
    // baseTest.cleanUp();
  }
}
