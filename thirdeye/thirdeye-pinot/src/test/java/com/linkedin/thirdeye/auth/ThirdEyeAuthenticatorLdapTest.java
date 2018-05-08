package com.linkedin.thirdeye.auth;

import com.google.common.base.Optional;
import io.dropwizard.auth.AuthenticationException;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.directory.DirContext;
import javax.naming.spi.InitialContextFactory;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class ThirdEyeAuthenticatorLdapTest {
  private static final Logger LOG = LoggerFactory.getLogger(ThirdEyeAuthenticatorLdapTest.class);
  private ThirdEyeAuthenticatorLdap thirdEyeAuthenticatorLdap;
  private Credentials credentials;

  private static String USERNAME1 = "username1"; // @DOMAIN1
  private static String USERNAME2 = "username2"; // @DOMAIN2
  private static String USERNAME3 = "username3"; // @DOMAIN3
  private static String PASSWORD = "not used";
  private static String DOMAIN1 = "domain1.do";
  private static String DOMAIN2 = "domain2.to";
  private static String DOMAIN3 = "domain3.so";

  @BeforeClass
  public void setup() {
    List<String> domains = Arrays.asList(DOMAIN1, DOMAIN2);
    thirdEyeAuthenticatorLdap = new ThirdEyeAuthenticatorLdap(domains, "ldaps://someLdap");
    thirdEyeAuthenticatorLdap.setInitialContextFactory(MockInitialDirContextFactory.class.getName());

    credentials = new Credentials();
    credentials.setPassword(PASSWORD);
  }

  @Test
  public void testBasicAuthentication() {
    // Test multiple domains
    try {
      credentials.setPrincipal(USERNAME1);
      Optional<ThirdEyePrincipal> authenticate = thirdEyeAuthenticatorLdap.authenticate(credentials);
      Assert.assertTrue(authenticate.isPresent(), "Authentication should not fail!");
    } catch (AuthenticationException e) {
      LOG.warn("Exception during authentication.", e);
      Assert.fail();
    }
    try {
      credentials.setPrincipal(USERNAME2);
      Optional<ThirdEyePrincipal> authenticate = thirdEyeAuthenticatorLdap.authenticate(credentials);
      Assert.assertTrue(authenticate.isPresent(), "Authentication should not fail!");
    } catch (AuthenticationException e) {
      LOG.warn("Exception during authentication.", e);
      Assert.fail();
    }

    // Test given domain name
    try {
      credentials.setPrincipal(USERNAME3 + '@' + DOMAIN3);
      Optional<ThirdEyePrincipal> authenticate = thirdEyeAuthenticatorLdap.authenticate(credentials);
      Assert.assertTrue(authenticate.isPresent(), "Authentication should not fail!");
    } catch (AuthenticationException e) {
      LOG.warn("Exception during authentication.", e);
      Assert.fail();
    }
  }

  @Test
  public void testFailedAuthentication() {
    // Failed reason: username 3 doesn't exist in domain1 and domain2
    try {
      credentials.setPrincipal(USERNAME3);
      Optional<ThirdEyePrincipal> authenticate = thirdEyeAuthenticatorLdap.authenticate(credentials);
      Assert.assertFalse(authenticate.isPresent(), "Authentication should fail!");
    } catch (AuthenticationException e) {
      LOG.warn("Exception during authentication.", e);
      Assert.fail();
    }
  }

  @Test
  public void testBlankAuthentication() {
    // Failed reason: blank username
    try {
      credentials.setPrincipal(null);
      Optional<ThirdEyePrincipal> authenticate = thirdEyeAuthenticatorLdap.authenticate(credentials);
      Assert.assertFalse(authenticate.isPresent(), "Authentication should fail!");
    } catch (AuthenticationException e) {
      LOG.warn("Exception during authentication.", e);
      Assert.fail();
    }
  }

  /**
   * Mocked LDAP server to testing purpose.
   */
  public static class MockInitialDirContextFactory implements InitialContextFactory {
    // Only USERNAME1@DOMAIN1, USERNAME2@DOMAIN2, USERNAME3@DOMAIN3 could be authenticated.
    public Context getInitialContext(Hashtable environment) throws NamingException {
      String principal = (String) environment.get(Context.SECURITY_PRINCIPAL);
      if (principal.equals(USERNAME1 + '@' + DOMAIN1) || principal.equals(USERNAME2 + '@' + DOMAIN2)
          || principal.equals(USERNAME3 + '@' + DOMAIN3)) {
        return Mockito.mock(DirContext.class);
      } else {
        throw new NamingException();
      }
    }
  }

}
