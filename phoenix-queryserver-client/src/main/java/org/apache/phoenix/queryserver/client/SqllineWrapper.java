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

import java.io.Console;
import java.security.PrivilegedExceptionAction;
import java.util.AbstractMap;
import java.util.Map.Entry;

import org.apache.calcite.avatica.BuiltInConnectionProperty;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import sqlline.SqlLine;

/**
 * Utility class which automatically performs a Kerberos login and then launches sqlline. Tries to
 * make a pre-populated ticket cache (via kinit before launching) transparently work.
 */
public class SqllineWrapper {
  public static final String HBASE_AUTHENTICATION_ATTR = "hbase.security.authentication";
  // Copied from QueryServices to avoid the dependency on phoenix-core
  public static final String QUERY_SERVER_BASIC_AUTHENTICATION_FILE = "phoenix.queryserver.basic.authentication.file";
  public static final String QUERY_SERVER_DIGEST_AUTHENTICATION_FILE = "phoenix.queryserver.digest.authentication.file";
  public static final String PASSWORD_PROMPT = "QueryServer user password: ";

  // Thin class around this method to make testing a bit easier
  static class Prompter {
    public String prompt(String prompt, boolean hide) {
      Console console = System.console();
      if (null == console) {
        throw new IllegalArgumentException("Console is not available to prompt user for input");
      }
      if (hide) {
        char[] password = console.readPassword(prompt);
        return new String(password);
      } else {
        return console.readLine(prompt);
      }
    }
  }

  static UserGroupInformation loginIfNecessary() {
    // Try to avoid HBase dependency too. Sadly, we have to bring in all of hadoop-common for this..
    Configuration conf = new Configuration(false);
    conf.addResource("hbase-site.xml");
    if ("kerberos".equalsIgnoreCase(conf.get(HBASE_AUTHENTICATION_ATTR))) {
      // sun.security.krb5.principal is the property for setting the principal name, if that
      // isn't set, fall back to user.name and hope for the best.
      String principal = System.getProperty("sun.security.krb5.principal", System.getProperty("user.name"));
      try {
        // We got hadoop-auth via hadoop-common, so might as well use it.
        return UserGroupInformation.getUGIFromTicketCache(null, principal);
      } catch (Exception e) {
        throw new RuntimeException("Kerberos login failed using ticket cache. Did you kinit?", e);
      }
    }
    return null;
  }

  private static String[] updateArgsForKerberos(String[] origArgs) {
    String[] newArgs = new String[origArgs.length];
    for (int i = 0; i < origArgs.length; i++) {
      String arg = origArgs[i];
      newArgs[i] = arg;

      if (arg.equals("-u")) {
        // Get the JDBC url which is the next argument
        i++;
        arg = origArgs[i];
        if (!arg.contains("authentication=")) {
          arg = arg + ";authentication=SPNEGO";
        }
        newArgs[i] = arg;
      }
    }
    return newArgs;
  }

  static String[] updateArgsWithPassword(String[] origArgs, Prompter prompter) {
    String[] newArgs = new String[origArgs.length];
    for (int i = 0; i < origArgs.length; i++) {
      String arg = origArgs[i];
      newArgs[i] = arg;

      if (arg.equals("-u")) {
        // Get the JDBC url which is the next argument
        i++;
        arg = origArgs[i];
        if (arg.contains("authentication=BASIC") || arg.contains("authentication=DIGEST")) {
          String avaticaPassword = prompter.prompt(PASSWORD_PROMPT, true);
          arg = arg + ";avatica_password=" + avaticaPassword;
        }
        newArgs[i] = arg;
      }
    }
    return newArgs;
  }

  static String prompt(String prompt, boolean hide) {
    Console console = System.console();
    if (null == console) {
      throw new IllegalArgumentException("Console is not available to prompt user for input");
    }
    if (hide) {
      char[] password = console.readPassword(prompt);
      return new String(password);
    } else {
      return console.readLine(prompt);
    }
  }

  /**
   * Checks if basic or digest, but not both, authentication are enabled in the provided configuration.
   * @param conf The configuration to check.
   * @return True if basic or digest is enabled, but not both.
   */
  static boolean isBasicOrDigestAuth(Configuration conf) {
    final String basicAuthFile = conf.get(QUERY_SERVER_BASIC_AUTHENTICATION_FILE);
    final String digestAuthFile = conf.get(QUERY_SERVER_DIGEST_AUTHENTICATION_FILE);
    if (null != basicAuthFile && null != digestAuthFile) {
      throw new IllegalArgumentException("Cannot use both HTTP Digest and Basic authentication");
    }

    // We know they are not both non-null, check if either are non-null
    return null != basicAuthFile || null != digestAuthFile;
  }

  public static void main(String[] args) throws Exception {
    UserGroupInformation ugi = loginIfNecessary();

    if (null != ugi) {
      final String[] updatedArgs = updateArgsForKerberos(args);
      ugi.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          SqlLine.main(updatedArgs);
          return null;
        }
      });
    } else {
      Configuration conf = new Configuration(false);
      conf.addResource("hbase-site.xml");
      // Check for HTTP Basic or Digest authentication, prompt the user to enter a password
      if (isBasicOrDigestAuth(conf)) {
        final String[] updatedArgs= updateArgsWithPassword(args, new Prompter());
        SqlLine.main(updatedArgs);
      } else {
        SqlLine.main(args);
      }
    }
  }
}
