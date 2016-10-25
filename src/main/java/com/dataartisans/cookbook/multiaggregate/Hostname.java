package com.dataartisans.cookbook.multiaggregate;

public class Hostname {
  private String host = "";
  private String domain = "";

  public Hostname() {
  }

  public Hostname(String host, String domain) {
    this.host = host;
    this.domain = domain;
  }

  public String getDomain() {
    return domain;
  }

  public void setDomain(String domain) {
    this.domain = domain;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  @Override
  public String toString() {
    return "Hostname{" +
      "host='" + host + '\'' +
      ", domain='" + domain + '\'' +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Hostname hostname = (Hostname) o;

    if (!host.equals(hostname.host)) return false;
    return domain.equals(hostname.domain);

  }

  @Override
  public int hashCode() {
    int result = host.hashCode();
    result = 31 * result + domain.hashCode();
    return result;
  }
}
