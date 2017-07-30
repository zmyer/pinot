package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.datalayer.dto.ApplicationDTO;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestApplicationManager extends AbstractManagerTestBase {

  Long applicationId;

  @BeforeClass
  void beforeClass() {
    super.init();
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    super.cleanup();
  }

  @Test
  public void testCreateApplication() {
    ApplicationDTO request = new ApplicationDTO();
    request.setApplication("MY_APP");
    request.setRecipients("abc@abc.in");
    applicationId = applicationDAO.save(request);
    Assert.assertTrue(applicationId > 0);
  }

  @Test(dependsOnMethods = { "testCreateApplication" })
  public void testFetchApplication() {
    // find by id
    ApplicationDTO response = applicationDAO.findById(applicationId);
    Assert.assertNotNull(response);
    Assert.assertEquals(response.getId(), applicationId);
    Assert.assertEquals(applicationDAO.findAll().size(), 1);
  }

  @Test(dependsOnMethods = { "testFetchApplication" })
  public void testDeleteApplication() {
    applicationDAO.deleteById(applicationId);
    Assert.assertEquals(applicationDAO.findAll().size(), 0);
  }
}
