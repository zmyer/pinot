package com.linkedin.thirdeye.auth;

import com.linkedin.thirdeye.dashboard.resources.v2.AuthResource;
import com.linkedin.thirdeye.datalayer.bao.SessionManager;
import com.linkedin.thirdeye.datalayer.dto.SessionDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import io.dropwizard.auth.AuthFilter;
import io.dropwizard.auth.Authenticator;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ThirdEyeAuthFilter extends AuthFilter<Credentials, ThirdEyePrincipal> {
  private static final Logger LOG = LoggerFactory.getLogger(ThirdEyeAuthFilter.class);

  private static final ThreadLocal<ThirdEyePrincipal> principalAuthContextThreadLocal = new ThreadLocal<>();
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  private final Set<String> allowedPaths;
  private final SessionManager sessionDAO;

  public ThirdEyeAuthFilter(Authenticator<Credentials, ThirdEyePrincipal> authenticator, Set<String> allowedPaths) {
    this.authenticator = authenticator;
    this.allowedPaths = allowedPaths;
    this.sessionDAO = DAO_REGISTRY.getSessionDAO();
  }

  @Override
  public void filter(ContainerRequestContext containerRequestContext) {
    setCurrentPrincipal(null);

    String uriPath = containerRequestContext.getUriInfo().getPath();
    LOG.info("Checking auth for {}", uriPath);

    ThirdEyePrincipal principal = new ThirdEyePrincipal();

    if (!isAuthenticated(containerRequestContext, principal)) {
      // not authenticated, check exceptions

      // authenticate end points should be out of auth filter
      if (uriPath.equals("auth")
          || uriPath.equals("auth/")
          || uriPath.equals("auth/authenticate")
          || uriPath.equals("auth/logout")
          // Landing page should not throw 401
          || uriPath.equals("thirdeye")
          // Let the FE handle the redirect to login page when not authenticated
          || uriPath.equals("thirdeye-admin")
          // Let detector capture the screenshot without authentication error
          || uriPath.startsWith("anomalies/search/anomalyIds")
          || uriPath.startsWith("thirdeye/email/generate/datasets")) {
        return;
      }

      for (String fragment : this.allowedPaths) {
        if (uriPath.startsWith(fragment)) {
          return;
        }
      }

      throw new WebApplicationException("Unable to validate credentials", Response.Status.UNAUTHORIZED);
    }

    setCurrentPrincipal(principal);
  }

  private boolean isAuthenticated(ContainerRequestContext containerRequestContext, ThirdEyePrincipal principal) {
    Map<String, Cookie> cookies = containerRequestContext.getCookies();

    if (cookies != null && cookies.containsKey(AuthResource.AUTH_TOKEN_NAME)) {
      String sessionKey = cookies.get(AuthResource.AUTH_TOKEN_NAME).getValue();
      if (sessionKey.isEmpty()) {
        LOG.error("Empty sessionKey. Skipping.");
      } else {
        SessionDTO sessionDTO = this.sessionDAO.findBySessionKey(sessionKey);
        if (sessionDTO != null && System.currentTimeMillis() < sessionDTO.getExpirationTime()) {
          // session exist in database and has not expired
          principal.setName(sessionDTO.getPrincipal());
          principal.setSessionKey(sessionKey);
          LOG.info("Found valid session {} for user {}", sessionDTO.getSessionKey(), sessionDTO.getPrincipal());
          return true;
        }
      }
    }
    return false;
  }

  private static void setCurrentPrincipal(ThirdEyePrincipal principal) {
    // TODO refactor this, use injectors
    principalAuthContextThreadLocal.set(principal);
  }

  public static ThirdEyePrincipal getCurrentPrincipal() {
    // TODO refactor this, use injectors
    return principalAuthContextThreadLocal.get();
  }
}
