/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.queryserver.server;

import org.apache.calcite.avatica.server.HttpServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.query.QueryServices;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertNotNull;
import static org.mockito.AdditionalMatchers.aryEq;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test class for {@link Main}.
 */
public class MainTest {

  private Main m;
  private Configuration conf;

  @Before
  public void setup() {
    m = new Main();
    conf = new Configuration(false);
    m.setConf(conf);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBasicAndDigestExclusive() {
    m.verifyBasicAuthConfiguration("/basic.properties", "/digest.properties", false);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBasicAndKerberosExclusive() {
    m.verifyBasicAuthConfiguration("/basic.properties", null, true);
  }

  @Test
  public void testBasicPropertiesFileExistence() throws URISyntaxException {
    try {
      m.verifyBasicAuthConfiguration("/foobarbazfake.properties", null, false);
      fail("Expected to fail when the configuration file does not exist");
    } catch (IllegalArgumentException e) {
      // Pass
    }

    URL url = getClass().getResource("/user.properties");
    assertNotNull("Could not load test resource file", url);
    File f = new File(url.toURI());
    m.verifyBasicAuthConfiguration(f.getAbsolutePath(), null, false);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDigestAndBasicExclusive() {
    m.verifyDigestAuthConfiguration("/basic.properties", "/digest.properties", false);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDigestAndKerberosExclusive() {
    m.verifyBasicAuthConfiguration(null, "/digest.properties", true);
  }

  @Test
  public void testDigestPropertiesFileExistence() throws URISyntaxException {
    try {
      m.verifyDigestAuthConfiguration("/foobarbazfake.properties", null, false);
      fail("Expected to fail when the configuration file does not exist");
    } catch (IllegalArgumentException e) {
      // Pass
    }

    URL url = getClass().getResource("/user.properties");
    assertNotNull("Could not load test resource file", url);
    File f = new File(url.toURI());
    m.verifyDigestAuthConfiguration(null, f.getAbsolutePath(), false);
  }

  @Test
  public void testBasicAuthHttpServerConfiguration() {
    final String basicAuthFile = "/basic.properties";
    final String[] roles = new String[] {"users", "admins"};
    HttpServer.Builder builder = mock(HttpServer.Builder.class);
    when(builder.withBasicAuthentication(eq(basicAuthFile), aryEq(roles))).thenReturn(builder);

    conf.set(QueryServices.QUERY_SERVER_BASIC_AUTHENTICATION_FILE, basicAuthFile);
    conf.setStrings(QueryServices.QUERY_SERVER_BASIC_AUTHENTICATION_ROLES, roles);
    m.configureBasicAuthServer(builder);

    verify(builder).withBasicAuthentication(eq(basicAuthFile), aryEq(roles));
  }

  @Test
  public void testBasicAuthHttpServerConfigurationWithNoRoles() {
    final String basicAuthFile = "/basic.properties";
    final String[] roles = new String[0];
    HttpServer.Builder builder = mock(HttpServer.Builder.class);
    when(builder.withBasicAuthentication(eq(basicAuthFile), aryEq(roles))).thenReturn(builder);

    conf.set(QueryServices.QUERY_SERVER_BASIC_AUTHENTICATION_FILE, basicAuthFile);
    m.configureBasicAuthServer(builder);

    verify(builder).withBasicAuthentication(eq(basicAuthFile), aryEq(roles));
  }

  @Test
  public void testDigestAuthHttpServerConfiguration() {
    final String digestAuthFile = "/digest.properties";
    final String[] roles = new String[] {"users", "admins"};
    HttpServer.Builder builder = mock(HttpServer.Builder.class);
    when(builder.withDigestAuthentication(eq(digestAuthFile), aryEq(roles))).thenReturn(builder);

    conf.set(QueryServices.QUERY_SERVER_DIGEST_AUTHENTICATION_FILE, digestAuthFile);
    conf.setStrings(QueryServices.QUERY_SERVER_DIGEST_AUTHENTICATION_ROLES, roles);
    m.configureDigestAuthServer(builder);

    verify(builder).withDigestAuthentication(eq(digestAuthFile), aryEq(roles));
  }

  @Test
  public void testDigestAuthHttpServerConfigurationWithNoRoles() {
    final String digestAuthFile = "/digest.properties";
    final String[] roles = new String[0];
    HttpServer.Builder builder = mock(HttpServer.Builder.class);
    when(builder.withDigestAuthentication(eq(digestAuthFile), aryEq(roles))).thenReturn(builder);

    conf.set(QueryServices.QUERY_SERVER_DIGEST_AUTHENTICATION_FILE, digestAuthFile);
    m.configureDigestAuthServer(builder);

    verify(builder).withDigestAuthentication(eq(digestAuthFile), aryEq(roles));
  }
}
