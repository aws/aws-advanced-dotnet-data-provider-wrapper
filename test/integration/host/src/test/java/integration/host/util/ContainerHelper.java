/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package integration.host.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.ExecCreateCmd;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.exception.DockerException;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import integration.host.TestInstanceInfo;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.InternetProtocol;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.containers.output.FrameConsumerResultCallback;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.images.builder.dockerfile.DockerfileBuilder;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;
import org.testcontainers.utility.TestEnvironment;

public class ContainerHelper {

  private static final String MYSQL_CONTAINER_IMAGE_NAME = "mysql:latest";
  private static final String POSTGRES_CONTAINER_IMAGE_NAME = "postgres:latest";
  private static final String MARIADB_CONTAINER_IMAGE_NAME = "mariadb:10";
  // Note: this image version may need to be occasionally updated to keep it up-to-date and prevent toxiproxy issues.
  private static final DockerImageName TOXIPROXY_IMAGE =
      DockerImageName.parse("ghcr.io/shopify/toxiproxy:latest");

  private static final int PROXY_CONTROL_PORT = 8474;
  private static final int PROXY_PORT = 8666;

  // Frameworks the integration suite runs against, in order. Defaults to net10.0 alone so the
  // automatic post-merge runs keep their current duration: every added framework repeats the whole
  // suite against the same cluster, roughly multiplying wall-clock time. Set TEST_FRAMEWORKS to a
  // comma separated list (for example "net10.0,net8.0") to widen it, which is what the manually
  // dispatched multi-framework runs do. Each framework needs a runtime in the test container image:
  // the base image supplies net10.0, and NET8_RUNTIME_INSTALL adds net8.0.
  private static final String CONTAINER_DEFAULT_TEST_FRAMEWORK = "net10.0";
  private static final String NET8_FRAMEWORK = "net8.0";
  private static final List<String> CONTAINER_TEST_FRAMEWORKS = resolveTestFrameworks();

  private static List<String> resolveTestFrameworks() {
    String configured = System.getenv("TEST_FRAMEWORKS");
    if (configured == null || configured.trim().isEmpty()) {
      return Collections.singletonList(CONTAINER_DEFAULT_TEST_FRAMEWORK);
    }

    List<String> frameworks = Arrays.stream(configured.split(","))
        .map(String::trim)
        .filter(framework -> !framework.isEmpty())
        .distinct()
        // The default framework is the only one that can run solution-wide, so it goes first: it is
        // the pass that covers the test projects which target it exclusively.
        .sorted(Comparator.comparingInt(
            framework -> CONTAINER_DEFAULT_TEST_FRAMEWORK.equals(framework) ? 0 : 1))
        .collect(Collectors.toList());

    return frameworks.isEmpty()
        ? Collections.singletonList(CONTAINER_DEFAULT_TEST_FRAMEWORK)
        : Collections.unmodifiableList(frameworks);
  }

  // Test projects that multitarget net8.0. Kept in sync with the TargetFrameworks in those csproj files.
  private static final List<String> NET8_TEST_PROJECTS =
      Collections.unmodifiableList(Arrays.asList(
          "AwsWrapperDataProvider.Tests",
          "AwsWrapperDataProvider.NHibernate.Tests"));

  // The dotnet/sdk:10.0 base image carries only the .NET 10 runtime, so the net8.0 test host would
  // fail to start. Only the runtime is installed (not a second SDK) since the 10.0 SDK builds both
  // frameworks, and it goes to the image's existing dotnet root so the installed muxer finds it.
  private static final String NET8_RUNTIME_INSTALL =
      "curl -fsSL https://dot.net/v1/dotnet-install.sh -o /tmp/dotnet-install.sh"
          + " && chmod +x /tmp/dotnet-install.sh"
          + " && /tmp/dotnet-install.sh --channel 8.0 --runtime dotnet --install-dir /usr/share/dotnet"
          + " && rm /tmp/dotnet-install.sh";

  private static final String XRAY_TELEMETRY_IMAGE_NAME = "amazon/aws-xray-daemon";
  private static final String OTLP_TELEMETRY_IMAGE_NAME = "amazon/aws-otel-collector";

  public Long runCmd(GenericContainer<?> container, String... cmd)
      throws IOException, InterruptedException {
    System.out.println("==== Container console feed ==== >>>>");
    Consumer<OutputFrame> consumer = new ConsoleConsumer();
    Long exitCode = execInContainer(container, consumer, cmd);
    System.out.println("==== Container console feed ==== <<<<");
    return exitCode;
  }

  public Long runCmdInDirectory(GenericContainer<?> container, String workingDirectory, String... cmd)
      throws IOException, InterruptedException {
    System.out.println("==== Container console feed ==== >>>>");
    Consumer<OutputFrame> consumer = new ConsoleConsumer();
    Long exitCode = execInContainer(container, workingDirectory, consumer, cmd);
    System.out.println("==== Container console feed ==== <<<<");
    return exitCode;
  }

  public void runTest(GenericContainer<?> container, String task, String engineDeployment)
      throws IOException, InterruptedException {
    runTest(container, task, engineDeployment, null, null);
  }

  public void runTest(GenericContainer<?> container, String task, String engineDeployment, String includeTags, String excludeTags)
      throws IOException, InterruptedException {
    System.out.println("==== Container console feed ==== >>>>");
    Consumer<OutputFrame> consumer = new ConsoleConsumer(true);
    execInContainer(container, consumer, "printenv", "TEST_ENV_DESCRIPTION");

    Long exitCode = execInContainer(container, consumer, "dotnet", "build");
    assertEquals(0, exitCode, "Dotnet build failed.");

    // For Entity Framework tests
    if (task.endsWith("ef")) {
        String efProject = "AwsWrapperDataProvider.EntityFrameworkCore.Tests";

        exitCode = execInContainer(container, consumer,
                "dotnet", "ef", "migrations", "add", "InitialCreate_" + System.currentTimeMillis(), "--project", efProject);
        assertEquals(0, exitCode, "Failed to generate Entity framework migration.");

        exitCode = execInContainer(container, consumer,
                "dotnet", "ef", "database", "update", "--project", efProject);
        assertEquals(0, exitCode, "Failed to update database with migration");
    }

    // Microsoft.Testing.Platform replaces the VSTest options that used to be passed here:
    // "--logger:console;verbosity=detailed" becomes "--output Detailed", and exit code 8
    // ("zero tests ran") has to be ignored because the filter runs solution-wide, so the test
    // projects holding no test for this task legitimately match nothing.
    //
    // The frameworks run one after another inside this same container, against the AWS resources
    // this task has already provisioned. Running them sequentially here rather than as separate
    // workflow jobs is what keeps a single Aurora cluster serving both, and it also keeps the two
    // passes from competing over the same cluster.
    String filter = "Category=Integration&Database=" + task + "&Engine=" + engineDeployment;
    Map<String, Long> exitCodesByFramework = new LinkedHashMap<>();

    for (String framework : CONTAINER_TEST_FRAMEWORKS) {
      List<String> projects = testProjectsForFramework(framework, task);
      if (projects.isEmpty()) {
        System.out.println("Skipping " + framework + ": no test project for task '" + task + "' targets it.");
        continue;
      }

      for (String project : projects) {
        List<String> command = new ArrayList<>(Arrays.asList("dotnet", "test"));
        // An empty project means "whatever `dotnet test` resolves here", i.e. the solution.
        if (!project.isEmpty()) {
          command.add(project);
        }
        command.addAll(Arrays.asList("--framework", framework, "--filter", filter));
        if (task.contains("perf")) {
          command.addAll(Arrays.asList("--configuration", "Release"));
        } else {
          command.add("--no-build");
        }
        command.addAll(Arrays.asList("--output", "Detailed", "--ignore-exit-code", "8"));

        String label = framework + (project.isEmpty() ? "" : " (" + project + ")");
        System.out.println("==== Running integration tests for " + label + " ====");
        Long frameworkExitCode = execInContainer(container, consumer, command.toArray(new String[0]));

        // Keep the first failure per framework, but carry on so one framework failing still reports
        // the other's result instead of hiding it.
        exitCodesByFramework.merge(label, frameworkExitCode, (existing, latest) -> existing != 0 ? existing : latest);
      }
    }

    System.out.println("==== Container console feed ==== <<<<");

    exitCodesByFramework.forEach(
        (label, code) -> System.out.println("Integration tests for " + label + " exited with " + code));

    String failed = exitCodesByFramework.entrySet().stream()
        .filter(entry -> entry.getValue() == null || entry.getValue() != 0)
        .map(Map.Entry::getKey)
        .collect(Collectors.joining(", "));

    assertTrue(failed.isEmpty(), "Some tests failed for: " + failed);
  }

  /**
   * Test projects to run for a given framework. net10.0 runs solution-wide so every test project is
   * covered. net8.0 has to name projects explicitly: a solution-wide "--framework net8.0" fails with
   * NETSDK1005 because AwsWrapperDataProvider.EntityFrameworkCore.Tests (EF Core 10 requires .NET 10)
   * and AwsWrapperDataProvider.Performance.Tests have no net8.0 target.
   */
  private static List<String> testProjectsForFramework(String framework, String task) {
    if (CONTAINER_DEFAULT_TEST_FRAMEWORK.equals(framework)) {
      return Collections.singletonList("");
    }

    // The EF and perf suites live solely in net10.0-only projects, so those tasks have nothing to run.
    if (task.endsWith("ef") || task.contains("perf")) {
      return Collections.emptyList();
    }

    return NET8_TEST_PROJECTS;
  }

  public void debugTest(GenericContainer<?> container, String task)
      throws IOException, InterruptedException {
    debugTest(container, task, null, null);
  }

  public void debugTest(GenericContainer<?> container, String task, String includeTags, String excludeTags)
      throws IOException, InterruptedException {
    System.out.println("==== Container console feed ==== >>>>");
    Consumer<OutputFrame> consumer = new ConsoleConsumer();
    execInContainer(container, consumer, "printenv", "TEST_ENV_DESCRIPTION");

    // Single framework on purpose: this is the debug entry point, so it stays fast rather than
    // repeating the unit suite per framework the way runTest does for integration coverage.
    Long exitCode = execInContainer(container, consumer, "dotnet", "test", "--framework", CONTAINER_DEFAULT_TEST_FRAMEWORK, "--filter", "Category!=Integration", "--ignore-exit-code", "8");
    System.out.println("==== Container console feed ==== <<<<");
    assertEquals(0, exitCode, "Some tests failed.");
  }

  // This container supports traces to AWS XRay.
  public GenericContainer<?> createTelemetryXrayContainer(
      String xrayAwsRegion,
      Network network,
      String networkAlias) {

    return new FixedExposedPortContainer<>(
        new ImageFromDockerfile("xray-daemon", true)
            .withDockerfileFromBuilder(
                builder -> builder
                        .from(XRAY_TELEMETRY_IMAGE_NAME)
                        .entryPoint("/xray",
                          "-t", "0.0.0.0:2000",
                          "-b", "0.0.0.0:2000",
                          "--local-mode",
                          "--log-level", "debug",
                          "--region", xrayAwsRegion)
                        .build()))
        .withExposedPort(2000)
        .waitingFor(Wait.forLogMessage(".*Starting proxy http server on 0.0.0.0:2000.*", 1))
        .withNetworkAliases(networkAlias)
        .withNetwork(network);
  }

  // This container supports traces and metrics to AWS CloudWatch/XRay
  public GenericContainer<?> createTelemetryOtlpContainer(
      Network network,
      String networkAlias) {

    return new FixedExposedPortContainer<>(DockerImageName.parse(OTLP_TELEMETRY_IMAGE_NAME))
        .withExposedPort(2000)
        .withExposedPort(1777)
        .withExposedPort(4317)
        .withExposedPort(4318)
        .waitingFor(Wait.forLogMessage(".*Everything is ready. Begin running and processing data.*", 1))
        .withNetworkAliases(networkAlias)
        .withNetwork(network)
        .withCopyFileToContainer(
            MountableFile.forHostPath("./src/test/resources/otel-config.yaml"),
            "/etc/otel-config.yaml");

  }

  public GenericContainer<?> createTestContainer(String dockerImageName, String testContainerImageName) {
    return createTestContainer(
        dockerImageName,
        testContainerImageName,
        builder -> builder // Return directly, do not append extra run commands to the docker builder.
    );
  }

  public GenericContainer<?> createTestContainer(
      String dockerImageName,
      String testContainerImageName,
      Function<DockerfileBuilder, DockerfileBuilder> appendExtraCommandsToBuilder) {
    class FixedExposedPortContainer<T extends GenericContainer<T>> extends GenericContainer<T> {

      public FixedExposedPortContainer(ImageFromDockerfile withDockerfileFromBuilder) {
        super(withDockerfileFromBuilder);
      }

      public T withFixedExposedPort(int hostPort, int containerPort) {
        super.addFixedExposedPort(hostPort, containerPort, InternetProtocol.TCP);

        return self();
      }
    }

    return new FixedExposedPortContainer<>(
        new ImageFromDockerfile(dockerImageName, true)
            .withDockerfileFromBuilder(
                builder -> {
                  DockerfileBuilder imageBuilder = builder.from(testContainerImageName);
                  // Only fetched when a net8.0 pass is actually requested, so the default runs do not
                  // take on a network download during image build.
                  if (CONTAINER_TEST_FRAMEWORKS.contains(NET8_FRAMEWORK)) {
                    imageBuilder = imageBuilder.run(NET8_RUNTIME_INSTALL);
                  }

                  appendExtraCommandsToBuilder.apply(
                    imageBuilder
                        .run("dotnet tool install --global dotnet-ef --version 10.0.12")
                        .env("PATH", "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/root/.dotnet/tools")
                        .run("mkdir", "app")
                        .workDir("/app")
                        .entryPoint("/bin/sh -c \"while true; do sleep 30; done;\"")
                        .expose(5005) // Exposing ports for debugger to be attached
                  ).build();
                }))
        .withFixedExposedPort(5005, 5005) // Mapping container port to host
        .withFileSystemBind("../../../AwsWrapperDataProvider.sln", "/app/AwsWrapperDataProvider.sln", BindMode.READ_ONLY)
        .withFileSystemBind("../../../.editorconfig", "/app/.editorconfig", BindMode.READ_ONLY)
        .withFileSystemBind("../../../Directory.Build.props", "/app/Directory.Build.props", BindMode.READ_ONLY)
        .withFileSystemBind("../../../global.json", "/app/global.json", BindMode.READ_ONLY)
        .withFileSystemBind("../../../AwsWrapperDataProvider.Benchmarks", "/app/AwsWrapperDataProvider.Benchmarks", BindMode.READ_WRITE)
        .withFileSystemBind("../../../AwsWrapperDataProvider.Dialect.MySqlClient", "/app/AwsWrapperDataProvider.Dialect.MySqlClient", BindMode.READ_WRITE)
        .withFileSystemBind("../../../AwsWrapperDataProvider.Dialect.MySqlConnector", "/app/AwsWrapperDataProvider.Dialect.MySqlConnector", BindMode.READ_WRITE)
        .withFileSystemBind("../../../AwsWrapperDataProvider.Dialect.Npgsql", "/app/AwsWrapperDataProvider.Dialect.Npgsql", BindMode.READ_WRITE)
        .withFileSystemBind("../../../AwsWrapperDataProvider.EntityFrameworkCore.Tests", "/app/AwsWrapperDataProvider.EntityFrameworkCore.Tests", BindMode.READ_WRITE)
        .withFileSystemBind("../../../AwsWrapperDataProvider.EntityFrameworkCore.MySqlConnector", "/app/AwsWrapperDataProvider.EntityFrameworkCore.MySqlConnector", BindMode.READ_WRITE)
        .withFileSystemBind("../../../AwsWrapperDataProvider.EntityFrameworkCore.PostgreSQL", "/app/AwsWrapperDataProvider.EntityFrameworkCore.PostgreSQL", BindMode.READ_WRITE)
        .withFileSystemBind("../../../AwsWrapperDataProvider", "/app/AwsWrapperDataProvider", BindMode.READ_WRITE)
        .withFileSystemBind("../../../AwsWrapperDataProvider.Authentication", "/app/AwsWrapperDataProvider.Authentication", BindMode.READ_WRITE)
        .withFileSystemBind("../../../AwsWrapperDataProvider.Plugin.Iam", "/app/AwsWrapperDataProvider.Plugin.Iam", BindMode.READ_WRITE)
        .withFileSystemBind("../../../AwsWrapperDataProvider.Plugin.SecretsManager", "/app/AwsWrapperDataProvider.Plugin.SecretsManager", BindMode.READ_WRITE)
        .withFileSystemBind("../../../AwsWrapperDataProvider.Plugin.FederatedAuth", "/app/AwsWrapperDataProvider.Plugin.FederatedAuth", BindMode.READ_WRITE)
        .withFileSystemBind("../../../AwsWrapperDataProvider.Plugin.CustomEndpoint", "/app/AwsWrapperDataProvider.Plugin.CustomEndpoint", BindMode.READ_WRITE)
        .withFileSystemBind("../../../AwsWrapperDataProvider.Plugin.KmsEncryption", "/app/AwsWrapperDataProvider.Plugin.KmsEncryption", BindMode.READ_WRITE)
        .withFileSystemBind("../../../AwsWrapperDataProvider.Telemetry.XRay", "/app/AwsWrapperDataProvider.Telemetry.XRay", BindMode.READ_WRITE)
        .withFileSystemBind("../../../AwsWrapperDataProvider.Telemetry.XRay.Tests", "/app/AwsWrapperDataProvider.Telemetry.XRay.Tests", BindMode.READ_WRITE)
        .withFileSystemBind("../../../AwsWrapperDataProvider.Tests", "/app/AwsWrapperDataProvider.Tests", BindMode.READ_WRITE)
        .withFileSystemBind("../../../AwsWrapperDataProvider.Performance.Tests", "/app/AwsWrapperDataProvider.Performance.Tests", BindMode.READ_WRITE)
        .withFileSystemBind("../../../AwsWrapperDataProvider.NHibernate", "/app/AwsWrapperDataProvider.NHibernate", BindMode.READ_WRITE)
        .withFileSystemBind("../../../AwsWrapperDataProvider.NHibernate.Tests", "/app/AwsWrapperDataProvider.NHibernate.Tests", BindMode.READ_WRITE)
        .withFileSystemBind("../gradle", "/app/gradle", BindMode.READ_WRITE)
        .withFileSystemBind("../../../test/integration/container", "/app/test/integration/container", BindMode.READ_WRITE)
        .withPrivilegedMode(true) // it's needed to control Linux core settings like TcpKeepAlive
        .withCopyFileToContainer(MountableFile.forHostPath("../gradlew"), "app/gradlew")
        .withCopyFileToContainer(
            MountableFile.forHostPath("./src/test/build.gradle.kts"), "app/build.gradle.kts")
        .withCopyFileToContainer(
            MountableFile.forHostPath("./src/test/resources/rds-ca-2019-root.pem"),
            "app/test/resources/rds-ca-2019-root.pem");
  }

  protected Long execInContainer(
      GenericContainer<?> container, String workingDirectory, Consumer<OutputFrame> consumer, String... command)
      throws UnsupportedOperationException, IOException, InterruptedException {
    return execInContainer(container.getContainerInfo(), consumer, workingDirectory, command);
  }

  protected Long execInContainer(
      GenericContainer<?> container,
      Consumer<OutputFrame> consumer,
      String... command)
      throws UnsupportedOperationException, IOException, InterruptedException {
    return execInContainer(container.getContainerInfo(), consumer, null, command);
  }

  protected Long execInContainer(
      InspectContainerResponse containerInfo,
      Consumer<OutputFrame> consumer,
      String workingDir,
      String... command)
      throws UnsupportedOperationException, IOException, InterruptedException {
    if (!TestEnvironment.dockerExecutionDriverSupportsExec()) {
      // at time of writing, this is the expected result in CircleCI.
      throw new UnsupportedOperationException(
          "Your docker daemon is running the \"lxc\" driver, which doesn't support \"docker exec\".");
    }

    if (!isRunning(containerInfo)) {
      throw new IllegalStateException(
          "execInContainer can only be used while the Container is running");
    }

    final String containerId = containerInfo.getId();
    final DockerClient dockerClient = DockerClientFactory.instance().client();
    final ExecCreateCmd cmd = dockerClient
        .execCreateCmd(containerId)
        .withAttachStdout(true)
        .withAttachStderr(true)
        .withCmd(command);

    if (!StringUtils.isNullOrEmpty(workingDir)) {
      cmd.withWorkingDir(workingDir);
    }

    final ExecCreateCmdResponse execCreateCmdResponse = cmd.exec();
    try (final FrameConsumerResultCallback callback = new FrameConsumerResultCallback()) {
      callback.addConsumer(OutputFrame.OutputType.STDOUT, consumer);
      callback.addConsumer(OutputFrame.OutputType.STDERR, consumer);
      dockerClient.execStartCmd(execCreateCmdResponse.getId()).exec(callback).awaitCompletion();
    }

    return dockerClient.inspectExecCmd(execCreateCmdResponse.getId()).exec().getExitCodeLong();
  }

  protected boolean isRunning(InspectContainerResponse containerInfo) {
    try {
      return containerInfo != null
          && containerInfo.getState() != null
          && containerInfo.getState().getRunning();
    } catch (DockerException e) {
      return false;
    }
  }

  public MySQLContainer<?> createMysqlContainer(
      Network network, String networkAlias, String testDbName, String username, String password) {

    return new MySQLContainer<>(MYSQL_CONTAINER_IMAGE_NAME)
        .withNetwork(network)
        .withNetworkAliases(networkAlias)
        .withDatabaseName(testDbName)
        .withPassword(password)
        .withUsername(username)
        .withEnv("MYSQL_ROOT_PASSWORD", password)
        .withCopyFileToContainer(
            MountableFile.forHostPath("./src/test/config/standard-mysql-grant-root.sql"),
            "/docker-entrypoint-initdb.d/standard-mysql-grant-root.sql")
        .withCommand(
            "--local_infile=1",
            "--max_allowed_packet=40M",
            "--max-connections=2048",
            "--secure-file-priv=/var/lib/mysql",
            "--log_bin_trust_function_creators=1",
            "--character-set-server=utf8mb4",
            "--collation-server=utf8mb4_0900_as_cs",
            "--skip-character-set-client-handshake",
            "--log-error-verbosity=4");
  }

  public PostgreSQLContainer<?> createPostgresContainer(
      Network network, String networkAlias, String testDbName, String username, String password) {

    return new PostgreSQLContainer<>(POSTGRES_CONTAINER_IMAGE_NAME)
        .withNetwork(network)
        .withNetworkAliases(networkAlias)
        .withDatabaseName(testDbName)
        .withUsername(username)
        .withPassword(password);
  }

  public MariaDBContainer<?> createMariadbContainer(
      Network network, String networkAlias, String testDbName, String username, String password) {

    return new MariaDBContainer<>(MARIADB_CONTAINER_IMAGE_NAME)
        .withNetwork(network)
        .withNetworkAliases(networkAlias)
        .withDatabaseName(testDbName)
        .withPassword(password)
        .withUsername(username);
  }

  public ToxiproxyContainer createAndStartProxyContainer(
      final Network network,
      String networkAlias,
      String networkUrl,
      String hostname,
      int port) throws IOException {
    final ToxiproxyContainer container =
        new ToxiproxyContainer(TOXIPROXY_IMAGE)
            .withNetwork(network)
            .withNetworkAliases(networkAlias, networkUrl);
    container.start();
    final ToxiproxyClient toxiproxyClient = new ToxiproxyClient(
        container.getHost(),
        container.getMappedPort(PROXY_CONTROL_PORT));
    this.createProxy(toxiproxyClient, hostname, port);
    return container;
  }

  public void createProxy(final ToxiproxyClient client, String hostname, int port)
      throws IOException {
    client.createProxy(
        hostname + ":" + port,
        "0.0.0.0:" + PROXY_PORT,
        hostname + ":" + port);
  }

  public ToxiproxyContainer createProxyContainer(
      final Network network, TestInstanceInfo instance, String proxyDomainNameSuffix) {
    return new ToxiproxyContainer(TOXIPROXY_IMAGE)
        .withNetwork(network)
        .withNetworkAliases(
            "proxy-instance-" + instance.getInstanceId(),
            instance.getHost() + proxyDomainNameSuffix);
  }

  public static class FixedExposedPortContainer<T extends FixedExposedPortContainer<T>> extends GenericContainer<T> {

    public FixedExposedPortContainer(ImageFromDockerfile withDockerfileFromBuilder) {
      super(withDockerfileFromBuilder);
    }

    public FixedExposedPortContainer(final DockerImageName dockerImageName) {
      super(dockerImageName);
    }

    public T withFixedExposedPort(int hostPort, int containerPort, InternetProtocol protocol) {
      super.addFixedExposedPort(hostPort, containerPort, protocol);
      return self();
    }

    public T withExposedPort(Integer port) {
      super.addExposedPort(port);
      return self();
    }
  }
}
