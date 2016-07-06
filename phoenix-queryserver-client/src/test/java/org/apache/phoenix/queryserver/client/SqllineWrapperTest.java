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
package org.apache.phoenix.queryserver.client;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.queryserver.client.SqllineWrapper.Prompter;
import org.junit.Test;


/**
 * Test class for {@link SqllineWrapper}.
 */
public class SqllineWrapperTest {
  
  @Test
  public void testNoAuthDoesNotAlterConfig() {
    Prompter prompter = mock(Prompter.class);
    String[] origArgs = new String[] {"-d", Driver.class.getName(), "-u",
        "jdbc:phoenix:thin:url=http://localhost:8765;serialization=PROTOBUF"};
    String[] newArgs = SqllineWrapper.updateArgsWithPassword(origArgs, prompter);
    assertArrayEquals(origArgs, newArgs);
  }

  @Test
  public void testBasicAuthGetsPassword() {
    final String password = "password";
    Prompter prompter = mock(Prompter.class);
    when(prompter.prompt(SqllineWrapper.PASSWORD_PROMPT, true)).thenReturn(password);
    String[] origArgs = new String[] {"-d", Driver.class.getName(), "-u",
        "jdbc:phoenix:thin:url=http://localhost:8765;serialization=PROTOBUF;authentication=BASIC;avatica_user=user1"};
    String[] newArgs = SqllineWrapper.updateArgsWithPassword(origArgs, prompter);

    assertEquals(origArgs.length, newArgs.length);
    newArgs[newArgs.length - 1].endsWith(";avatica_password=" + password);
  }

  @Test
  public void testDigestAuthGetsPassword() {
    final String password = "password";
    Prompter prompter = mock(Prompter.class);
    when(prompter.prompt(SqllineWrapper.PASSWORD_PROMPT, true)).thenReturn(password);
    String[] origArgs = new String[] {"-d", Driver.class.getName(), "-u",
        "jdbc:phoenix:thin:url=http://localhost:8765;serialization=PROTOBUF;authentication=DIGEST;avatica_user=user1"};
    String[] newArgs = SqllineWrapper.updateArgsWithPassword(origArgs, prompter);

    assertEquals(origArgs.length, newArgs.length);
    newArgs[newArgs.length - 1].endsWith(";avatica_password=" + password);
  }

  @Test
  public void testBasicOrDigestAuthCheck() {
    Configuration conf = new Configuration(false);

    conf.set(QueryServices.QUERY_SERVER_BASIC_AUTHENTICATION_FILE, "/foo");
    conf.unset(QueryServices.QUERY_SERVER_DIGEST_AUTHENTICATION_FILE);

    assertTrue(SqllineWrapper.isBasicOrDigestAuth(conf));

    conf.unset(QueryServices.QUERY_SERVER_BASIC_AUTHENTICATION_FILE);
    conf.set(QueryServices.QUERY_SERVER_DIGEST_AUTHENTICATION_FILE, "/foo");

    assertTrue(SqllineWrapper.isBasicOrDigestAuth(conf));

    conf.unset(QueryServices.QUERY_SERVER_BASIC_AUTHENTICATION_FILE);
    conf.unset(QueryServices.QUERY_SERVER_DIGEST_AUTHENTICATION_FILE);

    assertFalse(SqllineWrapper.isBasicOrDigestAuth(conf));

    conf.set(SqllineWrapper.HBASE_AUTHENTICATION_ATTR, "kerberos");

    assertFalse(SqllineWrapper.isBasicOrDigestAuth(conf));

    conf.unset(SqllineWrapper.HBASE_AUTHENTICATION_ATTR);
    conf.set(QueryServices.QUERY_SERVER_BASIC_AUTHENTICATION_FILE, "/foo");
    conf.set(QueryServices.QUERY_SERVER_DIGEST_AUTHENTICATION_FILE, "/foo");

    try {
      SqllineWrapper.isBasicOrDigestAuth(conf);
      fail("Expected to get an exception when both basic and digest auth are configured");
    } catch (IllegalArgumentException e) {
      // Pass
    }
  }
}
