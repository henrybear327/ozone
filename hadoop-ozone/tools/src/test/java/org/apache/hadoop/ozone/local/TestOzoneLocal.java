/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.local;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Tests for {@link OzoneLocal}.
 */
class TestOzoneLocal {

  @Test
  void localCommandMetadataIsPresentAndHidden() {
    Command command = OzoneLocal.class.getAnnotation(Command.class);

    assertNotNull(command);
    assertEquals("ozone local", command.name());
    assertTrue(command.hidden());
  }

  @Test
  void runCommandMetadataIsPresentAndHidden() {
    Command command = OzoneLocal.RunCommand.class.getAnnotation(Command.class);

    assertNotNull(command);
    assertEquals("run", command.name());
    assertTrue(command.hidden());
  }

  @Test
  void genericCliRegistersRunPlaceholder() {
    OzoneLocal local = new OzoneLocal();

    assertTrue(local.getCmd().getSubcommands().containsKey("run"));
  }

  @Test
  void rootHelpHidesRunPlaceholder() throws Exception {
    OzoneLocal local = new OzoneLocal();
    CommandLine commandLine = local.getCmd();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    commandLine.setOut(new PrintWriter(new OutputStreamWriter(out, UTF_8),
        true));
    commandLine.setErr(new PrintWriter(new OutputStreamWriter(err, UTF_8),
        true));

    int exitCode = local.execute(new String[] {"--help"});

    String help = out.toString(UTF_8.name());
    assertEquals(0, exitCode);
    assertTrue(help.contains("Usage: ozone local"));
    assertFalse(help.contains("run"));
    assertEquals("", err.toString(UTF_8.name()));
  }

  @Test
  void runCommandIsQuietNoOpPlaceholder() throws Exception {
    OzoneLocal local = new OzoneLocal();
    CommandLine commandLine = local.getCmd();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    commandLine.setOut(new PrintWriter(new OutputStreamWriter(out, UTF_8),
        true));
    commandLine.setErr(new PrintWriter(new OutputStreamWriter(err, UTF_8),
        true));

    int exitCode = local.execute(new String[] {"run"});

    assertEquals(0, exitCode);
    assertEquals("", out.toString(UTF_8.name()));
    assertEquals("", err.toString(UTF_8.name()));
  }

  @Test
  void resolveConfigUsesDefaults() {
    LocalOzoneClusterConfig config = resolve(Collections.emptyMap());

    assertEquals(LocalOzoneClusterConfig.DEFAULT_DATA_DIR,
        config.getDataDir());
    assertEquals(LocalOzoneClusterConfig.FormatMode.IF_NEEDED,
        config.getFormatMode());
    assertEquals(1, config.getDatanodes());
    assertEquals("127.0.0.1", config.getHost());
    assertEquals("0.0.0.0", config.getBindHost());
    assertEquals(0, config.getScmPort());
    assertEquals(0, config.getOmPort());
    assertEquals(0, config.getS3gPort());
    assertTrue(config.isS3gEnabled());
    assertFalse(config.isEphemeral());
    assertEquals(Duration.ofMinutes(2), config.getStartupTimeout());
    assertEquals("admin", config.getS3AccessKey());
    assertEquals("admin123", config.getS3SecretKey());
    assertEquals("us-east-1", config.getS3Region());
  }

  @Test
  void resolveConfigUsesEnvironmentOverrides() {
    Map<String, String> env = new HashMap<>();
    env.put(OzoneLocal.ENV_DATA_DIR, "target/ozone-local-test");
    env.put(OzoneLocal.ENV_FORMAT, "always");
    env.put(OzoneLocal.ENV_DATANODES, "2");
    env.put(OzoneLocal.ENV_HOST, "localhost");
    env.put(OzoneLocal.ENV_BIND_HOST, "127.0.0.1");
    env.put(OzoneLocal.ENV_SCM_PORT, "9860");
    env.put(OzoneLocal.ENV_OM_PORT, "9862");
    env.put(OzoneLocal.ENV_S3G_ENABLED, "false");
    env.put(OzoneLocal.ENV_S3G_PORT, "9878");
    env.put(OzoneLocal.ENV_EPHEMERAL, "true");
    env.put(OzoneLocal.ENV_STARTUP_TIMEOUT, "120s");
    env.put(OzoneLocal.ENV_S3_ACCESS_KEY, "dev");
    env.put(OzoneLocal.ENV_S3_SECRET_KEY, "devsecret");
    env.put(OzoneLocal.ENV_S3_REGION, "eu-central-1");

    LocalOzoneClusterConfig config = resolve(env);

    assertEquals(Paths.get("target/ozone-local-test")
        .toAbsolutePath().normalize(), config.getDataDir());
    assertEquals(LocalOzoneClusterConfig.FormatMode.ALWAYS,
        config.getFormatMode());
    assertEquals(2, config.getDatanodes());
    assertEquals("localhost", config.getHost());
    assertEquals("127.0.0.1", config.getBindHost());
    assertEquals(9860, config.getScmPort());
    assertEquals(9862, config.getOmPort());
    assertEquals(9878, config.getS3gPort());
    assertFalse(config.isS3gEnabled());
    assertTrue(config.isEphemeral());
    assertEquals(Duration.ofSeconds(120), config.getStartupTimeout());
    assertEquals("dev", config.getS3AccessKey());
    assertEquals("devsecret", config.getS3SecretKey());
    assertEquals("eu-central-1", config.getS3Region());
  }

  @Test
  void resolveConfigAllowsCliFlagsToOverrideEnvironment() {
    Map<String, String> env = new HashMap<>();
    env.put(OzoneLocal.ENV_DATA_DIR, "target/env-local");
    env.put(OzoneLocal.ENV_FORMAT, "never");
    env.put(OzoneLocal.ENV_DATANODES, "2");
    env.put(OzoneLocal.ENV_HOST, "env-host");
    env.put(OzoneLocal.ENV_BIND_HOST, "0.0.0.0");
    env.put(OzoneLocal.ENV_SCM_PORT, "100");
    env.put(OzoneLocal.ENV_OM_PORT, "101");
    env.put(OzoneLocal.ENV_S3G_ENABLED, "true");
    env.put(OzoneLocal.ENV_S3G_PORT, "102");
    env.put(OzoneLocal.ENV_EPHEMERAL, "false");
    env.put(OzoneLocal.ENV_STARTUP_TIMEOUT, "30s");
    env.put(OzoneLocal.ENV_S3_ACCESS_KEY, "env-access");
    env.put(OzoneLocal.ENV_S3_SECRET_KEY, "env-secret");
    env.put(OzoneLocal.ENV_S3_REGION, "env-region");

    LocalOzoneClusterConfig config = resolve(env,
        "--data-dir", "target/cli-local",
        "--format", "always",
        "--datanodes", "3",
        "--host", "cli-host",
        "--bind-host", "127.0.0.1",
        "--scm-port", "200",
        "--om-port", "201",
        "--s3g-port", "202",
        "--without-s3g",
        "--ephemeral",
        "--startup-timeout", "PT45S",
        "--s3-access-key", "cli-access",
        "--s3-secret-key", "cli-secret",
        "--s3-region", "cli-region");

    assertEquals(Paths.get("target/cli-local").toAbsolutePath().normalize(),
        config.getDataDir());
    assertEquals(LocalOzoneClusterConfig.FormatMode.ALWAYS,
        config.getFormatMode());
    assertEquals(3, config.getDatanodes());
    assertEquals("cli-host", config.getHost());
    assertEquals("127.0.0.1", config.getBindHost());
    assertEquals(200, config.getScmPort());
    assertEquals(201, config.getOmPort());
    assertEquals(202, config.getS3gPort());
    assertFalse(config.isS3gEnabled());
    assertTrue(config.isEphemeral());
    assertEquals(Duration.ofSeconds(45), config.getStartupTimeout());
    assertEquals("cli-access", config.getS3AccessKey());
    assertEquals("cli-secret", config.getS3SecretKey());
    assertEquals("cli-region", config.getS3Region());
  }

  @Test
  void resolveConfigTreatsBlankEnvironmentValuesAsUnset() {
    Map<String, String> env = new HashMap<>();
    env.put(OzoneLocal.ENV_DATA_DIR, "   ");
    env.put(OzoneLocal.ENV_DATANODES, "\t");
    env.put(OzoneLocal.ENV_S3_ACCESS_KEY, "");
    env.put(OzoneLocal.ENV_S3_REGION, "  ");

    LocalOzoneClusterConfig config = resolve(env);

    assertEquals(LocalOzoneClusterConfig.DEFAULT_DATA_DIR,
        config.getDataDir());
    assertEquals(1, config.getDatanodes());
    assertEquals("admin", config.getS3AccessKey());
    assertEquals("us-east-1", config.getS3Region());
  }

  @Test
  void resolveConfigRejectsInvalidBooleanEnvironmentValue() {
    assertConfigError(OzoneLocal.ENV_S3G_ENABLED, "sometimes",
        OzoneLocal.ENV_S3G_ENABLED);
  }

  @Test
  void resolveConfigRejectsInvalidFormatEnvironmentValue() {
    assertConfigError(OzoneLocal.ENV_FORMAT, "sometimes",
        OzoneLocal.ENV_FORMAT);
  }

  @Test
  void resolveConfigRejectsInvalidIntegerEnvironmentValue() {
    assertConfigError(OzoneLocal.ENV_DATANODES, "two",
        OzoneLocal.ENV_DATANODES);
  }

  @Test
  void resolveConfigRejectsInvalidPortEnvironmentValue() {
    assertConfigError(OzoneLocal.ENV_SCM_PORT, "65536",
        OzoneLocal.ENV_SCM_PORT);
  }

  @Test
  void resolveConfigRejectsDatanodeCountBelowOne() {
    assertConfigError(OzoneLocal.ENV_DATANODES, "0",
        OzoneLocal.ENV_DATANODES);
  }

  @Test
  void resolveConfigRejectsInvalidDurationEnvironmentValue() {
    assertConfigError(OzoneLocal.ENV_STARTUP_TIMEOUT, "forever",
        OzoneLocal.ENV_STARTUP_TIMEOUT);
  }

  @Test
  void resolveConfigRejectsNonPositiveDurationEnvironmentValue() {
    assertConfigError(OzoneLocal.ENV_STARTUP_TIMEOUT, "0s",
        OzoneLocal.ENV_STARTUP_TIMEOUT);
  }

  @Test
  void resolveConfigRejectsInvalidPathEnvironmentValue() {
    assertConfigError(OzoneLocal.ENV_DATA_DIR, "\0",
        OzoneLocal.ENV_DATA_DIR);
  }

  @Test
  void resolveConfigRejectsInvalidPathCliValue() {
    assertCliConfigError("--data-dir", "\0", "--data-dir");
  }

  @Test
  void genericCliErrorOutputIncludesOffendingConfigSource()
      throws Exception {
    Map<String, String> env = new HashMap<>();
    env.put(OzoneLocal.ENV_S3G_ENABLED, "sometimes");
    OzoneLocal local = localWithEnvironment(env);
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    local.getCmd().setErr(new PrintWriter(new OutputStreamWriter(err, UTF_8),
        true));

    int exitCode = local.execute(new String[] {"run"});

    assertEquals(-1, exitCode);
    assertTrue(err.toString(UTF_8.name())
        .contains(OzoneLocal.ENV_S3G_ENABLED));
  }

  private static LocalOzoneClusterConfig resolve(Map<String, String> env,
      String... args) {
    OzoneLocal.RunCommand command = new OzoneLocal.RunCommand(env);
    new CommandLine(command).parseArgs(args);
    return command.resolveConfig(new OzoneConfiguration());
  }

  private static void assertConfigError(String key, String value,
      String expectedMessage) {
    Map<String, String> env = new HashMap<>();
    env.put(key, value);
    OzoneLocal.RunCommand command = new OzoneLocal.RunCommand(env);

    IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
        () -> command.resolveConfig(new OzoneConfiguration()));

    assertTrue(error.getMessage().contains(expectedMessage),
        error.getMessage());
  }

  private static void assertCliConfigError(String option, String value,
      String expectedMessage) {
    OzoneLocal.RunCommand command = new OzoneLocal.RunCommand(
        Collections.emptyMap());
    new CommandLine(command).parseArgs(option, value);

    IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
        () -> command.resolveConfig(new OzoneConfiguration()));

    assertTrue(error.getMessage().contains(expectedMessage),
        error.getMessage());
  }

  private static OzoneLocal localWithEnvironment(Map<String, String> env) {
    CommandLine.IFactory defaultFactory = CommandLine.defaultFactory();
    return new OzoneLocal(new CommandLine.IFactory() {
      @Override
      public <K> K create(Class<K> clazz) throws Exception {
        if (clazz == OzoneLocal.RunCommand.class) {
          return clazz.cast(new OzoneLocal.RunCommand(env));
        }
        return defaultFactory.create(clazz);
      }
    });
  }
}
