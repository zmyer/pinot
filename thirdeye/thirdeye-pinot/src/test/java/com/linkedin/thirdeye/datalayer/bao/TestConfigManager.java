package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.datalayer.dto.ConfigDTO;
import org.h2.jdbc.JdbcSQLException;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestConfigManager extends AbstractManagerTestBase {
  @BeforeMethod
  void beforeMethod() {
    super.init();
  }

  @AfterMethod(alwaysRun = true)
  void afterMethod() {
    super.cleanup();
  }

  @Test
  public void testCreateConfig() {
    ConfigDTO config = super.getTestConfig("a", "b", "c");
    Assert.assertNotNull(this.configDAO.save(config));
  }

  @Test
  public void testOverrideConfigFail() throws Exception {
    ConfigDTO first = super.getTestConfig("a", "b", "c");
    this.configDAO.save(first);

    ConfigDTO second = super.getTestConfig("a", "b", "OTHER");
    Assert.assertNull(this.configDAO.save(second));
  }

  @Test
  public void testDeleteConfig() {
    ConfigDTO config = super.getTestConfig("a", "b", "c");
    Assert.assertNotNull(this.configDAO.save(config));

    this.configDAO.deleteByNamespaceName("a", "b");

    Assert.assertNull(this.configDAO.findByNamespaceName("a", "b"));
  }

  @Test
  public void testOverrideWithDelete() {
    ConfigDTO config = super.getTestConfig("a", "b", "c");
    Assert.assertNotNull(this.configDAO.save(config));

    this.configDAO.deleteByNamespaceName("a", "b");

    ConfigDTO config2 = super.getTestConfig("a", "b", "xyz");
    Assert.assertNotNull(this.configDAO.save(config2));

    ConfigDTO out = this.configDAO.findByNamespaceName("a", "b");
    Assert.assertEquals(config2, out);
  }

  @Test
  public void testGetConfig() {
    ConfigDTO in = super.getTestConfig("a", "b", "c");
    Assert.assertNotNull(this.configDAO.save(in));

    ConfigDTO out = this.configDAO.findByNamespaceName("a", "b");
    Assert.assertEquals(in, out);
  }

  @Test
  public void testNamespace() {
    this.configDAO.save(this.getTestConfig("a", "a", "v1"));
    this.configDAO.save(this.getTestConfig("a", "b", "v2"));
    this.configDAO.save(this.getTestConfig("b", "a", "v3"));
    this.configDAO.save(this.getTestConfig("", "a", "v4"));

    Assert.assertEquals(this.configDAO.findByNamespace("a").size(), 2);
    Assert.assertEquals(this.configDAO.findByNamespace("b").size(), 1);
  }
}
