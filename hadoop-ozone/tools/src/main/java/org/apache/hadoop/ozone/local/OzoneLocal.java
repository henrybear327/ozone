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

import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Internal CLI entry point for local single-node Ozone commands.
 */
@Command(name = "ozone local",
    hidden = true,
    description = "Internal commands for local single-node Ozone",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    subcommands = {
        OzoneLocal.RunCommand.class
    })
public class OzoneLocal extends GenericCli {

  static final String ENV_DATA_DIR = "OZONE_LOCAL_DATA_DIR";
  static final String ENV_FORMAT = "OZONE_LOCAL_FORMAT";
  static final String ENV_DATANODES = "OZONE_LOCAL_DATANODES";
  static final String ENV_HOST = "OZONE_LOCAL_HOST";
  static final String ENV_BIND_HOST = "OZONE_LOCAL_BIND_HOST";
  static final String ENV_SCM_PORT = "OZONE_LOCAL_SCM_PORT";
  static final String ENV_OM_PORT = "OZONE_LOCAL_OM_PORT";
  static final String ENV_S3G_ENABLED = "OZONE_LOCAL_S3G_ENABLED";
  static final String ENV_S3G_PORT = "OZONE_LOCAL_S3G_PORT";
  static final String ENV_EPHEMERAL = "OZONE_LOCAL_EPHEMERAL";
  static final String ENV_STARTUP_TIMEOUT = "OZONE_LOCAL_STARTUP_TIMEOUT";
  static final String ENV_S3_ACCESS_KEY = "OZONE_LOCAL_S3_ACCESS_KEY";
  static final String ENV_S3_SECRET_KEY = "OZONE_LOCAL_S3_SECRET_KEY";
  static final String ENV_S3_REGION = "OZONE_LOCAL_S3_REGION";

  public OzoneLocal() {
    super();
  }

  OzoneLocal(CommandLine.IFactory factory) {
    super(factory);
  }

  public static void main(String[] args) {
    new OzoneLocal().run(args);
  }

  @Command(name = "run",
      hidden = true,
      description = "Internal placeholder for a local Ozone runtime")
  static class RunCommand implements Callable<Void> {

    private final Map<String, String> environment;

    @Option(names = "--data-dir",
        description = "Persistent data directory for the local cluster")
    private String dataDir;

    @Option(names = "--format",
        description = "Storage init mode: if-needed, always, never")
    private String format;

    @Option(names = "--datanodes",
        description = "Number of datanodes to start")
    private Integer datanodes;

    @Option(names = "--host",
        description = "Advertised host to write into local service addresses")
    private String host;

    @Option(names = "--bind-host",
        description = "Bind host for HTTP and RPC listeners")
    private String bindHost;

    @Option(names = "--scm-port",
        description = "SCM client RPC port (0 means auto-allocate)")
    private Integer scmPort;

    @Option(names = "--om-port",
        description = "OM RPC port (0 means auto-allocate)")
    private Integer omPort;

    @Option(names = "--s3g-port",
        description = "S3 Gateway HTTP port (0 means auto-allocate)")
    private Integer s3gPort;

    @Option(names = "--without-s3g",
        description = "Disable S3 Gateway")
    private boolean withoutS3g;

    @Option(names = "--ephemeral",
        description = "Delete the data directory on shutdown")
    private boolean ephemeral;

    @Option(names = "--startup-timeout",
        description = "How long to wait for the local cluster to become ready")
    private String startupTimeout;

    @Option(names = "--s3-access-key",
        description = "Suggested local AWS access key to print on startup")
    private String s3AccessKey;

    @Option(names = "--s3-secret-key",
        description = "Suggested local AWS secret key to print on startup")
    private String s3SecretKey;

    @Option(names = "--s3-region",
        description = "Suggested local AWS region to print on startup")
    private String s3Region;

    RunCommand() {
      this(System.getenv());
    }

    RunCommand(Map<String, String> environment) {
      this.environment = Objects.requireNonNull(environment, "environment");
    }

    @Override
    public Void call() {
      resolveConfig(new OzoneConfiguration());
      return null;
    }

    LocalOzoneClusterConfig resolveConfig(
        OzoneConfiguration baseConfiguration) {
      Path resolvedDataDir = resolvePath(dataDir, environment.get(ENV_DATA_DIR),
          "--data-dir", ENV_DATA_DIR, LocalOzoneClusterConfig.DEFAULT_DATA_DIR);
      LocalOzoneClusterConfig.FormatMode resolvedFormat = format != null
          ? parseFormat(format, "--format")
          : parseFormat(environment.get(ENV_FORMAT), ENV_FORMAT,
              LocalOzoneClusterConfig.DEFAULT_FORMAT_MODE);
      int resolvedDatanodes = datanodes != null
          ? datanodes
          : parseInt(environment.get(ENV_DATANODES), ENV_DATANODES,
              LocalOzoneClusterConfig.DEFAULT_DATANODES);
      if (resolvedDatanodes < 1) {
        throw new IllegalArgumentException(
            "Datanode count for "
                + (datanodes != null ? "--datanodes" : ENV_DATANODES)
                + " must be at least 1.");
      }

      String resolvedHost = firstNonBlank(host, environment.get(ENV_HOST),
          LocalOzoneClusterConfig.DEFAULT_HOST);
      String resolvedBindHost = firstNonBlank(bindHost,
          environment.get(ENV_BIND_HOST),
          LocalOzoneClusterConfig.DEFAULT_BIND_HOST);

      int resolvedScmPort = scmPort != null
          ? validatePort(scmPort, "--scm-port")
          : parsePort(environment.get(ENV_SCM_PORT), ENV_SCM_PORT,
              LocalOzoneClusterConfig.DEFAULT_PORT);
      int resolvedOmPort = omPort != null
          ? validatePort(omPort, "--om-port")
          : parsePort(environment.get(ENV_OM_PORT), ENV_OM_PORT,
              LocalOzoneClusterConfig.DEFAULT_PORT);
      int resolvedS3gPort = s3gPort != null
          ? validatePort(s3gPort, "--s3g-port")
          : parsePort(environment.get(ENV_S3G_PORT), ENV_S3G_PORT,
              LocalOzoneClusterConfig.DEFAULT_PORT);

      boolean resolvedS3gEnabled = withoutS3g ? false
          : parseBoolean(environment.get(ENV_S3G_ENABLED), ENV_S3G_ENABLED,
              LocalOzoneClusterConfig.DEFAULT_S3G_ENABLED);
      boolean resolvedEphemeral = ephemeral || parseBoolean(
          environment.get(ENV_EPHEMERAL), ENV_EPHEMERAL,
          LocalOzoneClusterConfig.DEFAULT_EPHEMERAL);

      Duration resolvedStartupTimeout = startupTimeout != null
          ? parseDuration(startupTimeout, "--startup-timeout",
              baseConfiguration)
          : parseDuration(environment.get(ENV_STARTUP_TIMEOUT),
              ENV_STARTUP_TIMEOUT,
              LocalOzoneClusterConfig.DEFAULT_STARTUP_TIMEOUT,
              baseConfiguration);
      if (resolvedStartupTimeout.isZero()
          || resolvedStartupTimeout.isNegative()) {
        throw new IllegalArgumentException("Startup timeout for "
            + (startupTimeout != null ? "--startup-timeout"
                : ENV_STARTUP_TIMEOUT)
            + " must be greater than zero.");
      }

      String resolvedAccessKey = firstNonBlank(s3AccessKey,
          environment.get(ENV_S3_ACCESS_KEY),
          LocalOzoneClusterConfig.DEFAULT_S3_ACCESS_KEY);
      String resolvedSecretKey = firstNonBlank(s3SecretKey,
          environment.get(ENV_S3_SECRET_KEY),
          LocalOzoneClusterConfig.DEFAULT_S3_SECRET_KEY);
      String resolvedRegion = firstNonBlank(s3Region,
          environment.get(ENV_S3_REGION),
          LocalOzoneClusterConfig.DEFAULT_S3_REGION);

      return LocalOzoneClusterConfig.builder(resolvedDataDir)
          .setFormatMode(resolvedFormat)
          .setDatanodes(resolvedDatanodes)
          .setHost(resolvedHost)
          .setBindHost(resolvedBindHost)
          .setScmPort(resolvedScmPort)
          .setOmPort(resolvedOmPort)
          .setS3gPort(resolvedS3gPort)
          .setS3gEnabled(resolvedS3gEnabled)
          .setEphemeral(resolvedEphemeral)
          .setStartupTimeout(resolvedStartupTimeout)
          .setS3AccessKey(resolvedAccessKey)
          .setS3SecretKey(resolvedSecretKey)
          .setS3Region(resolvedRegion)
          .build();
    }

    private static String firstNonBlank(String... values) {
      for (String value : values) {
        if (!isBlank(value)) {
          return trimWhitespace(value);
        }
      }
      return null;
    }

    private static Path resolvePath(String cliValue, String envValue,
        String cliSource, String envSource, Path fallback) {
      if (!isBlank(cliValue)) {
        return parsePath(trimWhitespace(cliValue), cliSource);
      }
      if (!isBlank(envValue)) {
        return parsePath(trimWhitespace(envValue), envSource);
      }
      return fallback.toAbsolutePath().normalize();
    }

    private static Path parsePath(String rawValue, String source) {
      try {
        return Paths.get(rawValue).toAbsolutePath().normalize();
      } catch (InvalidPathException ex) {
        throw new IllegalArgumentException("Unable to parse " + source
            + " as a filesystem path: " + rawValue, ex);
      }
    }

    private static LocalOzoneClusterConfig.FormatMode parseFormat(
        String rawValue, String source) {
      try {
        return LocalOzoneClusterConfig.FormatMode.fromString(rawValue);
      } catch (IllegalArgumentException ex) {
        throw new IllegalArgumentException("Unable to parse " + source
            + ". Expected one of: if-needed, always, never.", ex);
      }
    }

    private static LocalOzoneClusterConfig.FormatMode parseFormat(
        String rawValue, String source,
        LocalOzoneClusterConfig.FormatMode fallback) {
      if (isBlank(rawValue)) {
        return fallback;
      }
      return parseFormat(rawValue, source);
    }

    private static int parseInt(String rawValue, String source, int fallback) {
      if (isBlank(rawValue)) {
        return fallback;
      }
      try {
        return Integer.parseInt(rawValue.trim());
      } catch (NumberFormatException ex) {
        throw new IllegalArgumentException("Unable to parse " + source
            + " as an integer: " + rawValue, ex);
      }
    }

    private static boolean parseBoolean(String rawValue, String source,
        boolean fallback) {
      if (isBlank(rawValue)) {
        return fallback;
      }
      String value = rawValue.trim();
      if ("true".equalsIgnoreCase(value)
          || "yes".equalsIgnoreCase(value)
          || "on".equalsIgnoreCase(value)) {
        return true;
      }
      if ("false".equalsIgnoreCase(value)
          || "no".equalsIgnoreCase(value)
          || "off".equalsIgnoreCase(value)) {
        return false;
      }
      throw new IllegalArgumentException("Unable to parse " + source
          + " as a boolean. Use true/false, yes/no, or on/off.");
    }

    private static int parsePort(String rawValue, String source, int fallback) {
      return isBlank(rawValue) ? fallback
          : validatePort(parseInt(rawValue, source, fallback), source);
    }

    private static int validatePort(int value, String source) {
      if (value < 0 || value > 65_535) {
        throw new IllegalArgumentException("Port value for " + source
            + " must be between 0 and 65535.");
      }
      return value;
    }

    private static Duration parseDuration(String rawValue, String source,
        Duration fallback, OzoneConfiguration baseConfiguration) {
      if (isBlank(rawValue)) {
        return fallback;
      }
      return parseDuration(rawValue, source, baseConfiguration);
    }

    private static Duration parseDuration(String rawValue, String source,
        OzoneConfiguration baseConfiguration) {
      try {
        return Duration.parse(rawValue.trim());
      } catch (DateTimeParseException ignored) {
        OzoneConfiguration conf = new OzoneConfiguration(baseConfiguration);
        String configKey = "ozone.local.duration.parse";
        conf.set(configKey, rawValue.trim());
        long millis;
        try {
          millis = conf.getTimeDuration(configKey, -1L,
              TimeUnit.MILLISECONDS);
        } catch (RuntimeException ex) {
          throw new IllegalArgumentException(durationMessage(source), ex);
        }
        if (millis < 0) {
          throw new IllegalArgumentException(durationMessage(source));
        }
        return Duration.ofMillis(millis);
      }
    }

    private static String durationMessage(String source) {
      return "Unable to parse " + source
          + " as a duration. Use ISO-8601 like PT2M or "
          + "Hadoop-style values like 120s.";
    }

    private static boolean isBlank(String value) {
      return value == null || trimWhitespace(value).isEmpty();
    }

    private static String trimWhitespace(String value) {
      int start = 0;
      int end = value.length();
      while (start < end && Character.isWhitespace(value.charAt(start))) {
        start++;
      }
      while (end > start && Character.isWhitespace(value.charAt(end - 1))) {
        end--;
      }
      return value.substring(start, end);
    }
  }
}
