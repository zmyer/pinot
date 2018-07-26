package com.linkedin.thirdeye.datalayer.pojo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Objects;


/**
 * ConfigBean holds namespaced key-value configuration values.  Values are serialized into the
 * database using the default object mapper.  ConfigBean serves as a light-weight
 * alternative to existing configuration mechanisms to (a) allow at-runtime changes to configuration
 * traditionally stored in config files, and (b) alleviate the need for introducing new bean classes
 * to handle simple configuration tasks.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SessionBean extends AbstractBean {

  public enum PrincipalType {
    SERVICE,
    USER
  }

  String sessionKey;
  String principal;
  PrincipalType principalType;
  long expirationTime;

  public String getSessionKey() {
    return sessionKey;
  }

  public void setSessionKey(String sessionKey) {
    this.sessionKey = sessionKey;
  }

  public String getPrincipal() {
    return principal;
  }

  public void setPrincipal(String principal) {
    this.principal = principal;
  }

  public PrincipalType getPrincipalType() {
    return principalType;
  }

  public void setPrincipalType(PrincipalType principalType) {
    this.principalType = principalType;
  }

  public long getExpirationTime() {
    return expirationTime;
  }

  public void setExpirationTime(long expirationTime) {
    this.expirationTime = expirationTime;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SessionBean)) {
      return false;
    }
    SessionBean that = (SessionBean) o;
    return expirationTime == that.expirationTime && Objects.equals(sessionKey, that.sessionKey) && Objects.equals(
        principal, that.principal) && Objects.equals(principalType, that.principalType);
  }

  @Override
  public int hashCode() {

    return Objects.hash(sessionKey, principal, principalType, expirationTime);
  }
}
