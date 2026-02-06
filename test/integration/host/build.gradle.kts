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

import org.gradle.api.tasks.testing.logging.TestExceptionFormat.*
import org.gradle.api.tasks.testing.logging.TestLogEvent.*

plugins {
    id("java")
}

group = "software.amazon.go.integration.tests"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation("org.checkerframework:checker-qual:3.26.0")
    testImplementation("org.junit.platform:junit-platform-commons:1.9.0")
    testImplementation("org.junit.platform:junit-platform-engine:1.9.0")
    testImplementation("org.junit.platform:junit-platform-launcher:1.9.0")
    testImplementation("org.junit.platform:junit-platform-suite-engine:1.9.0")
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.9.1")
    testImplementation("org.junit.jupiter:junit-jupiter-params:5.9.1")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")

    testImplementation("org.apache.commons:commons-dbcp2:2.9.0")
    testImplementation("org.apache.commons:commons-lang3:3.18.0")
    testImplementation("org.postgresql:postgresql:42.5.0")
    testImplementation("mysql:mysql-connector-java:8.0.30")
    testImplementation("org.mockito:mockito-inline:4.8.0")
    testImplementation("software.amazon.awssdk:rds:2.31.64")
    testImplementation("software.amazon.awssdk:ec2:2.31.64")
    testImplementation("software.amazon.awssdk:secretsmanager:2.31.64")
    testImplementation("org.apache.poi:poi-ooxml:5.2.2")
    testImplementation("org.slf4j:slf4j-simple:2.0.3")
    testImplementation("com.fasterxml.jackson.core:jackson-databind:2.14.2")
    testImplementation("com.amazonaws:aws-xray-recorder-sdk-core:2.14.0")
    testImplementation("io.opentelemetry:opentelemetry-sdk:1.29.0")
    testImplementation("io.opentelemetry:opentelemetry-sdk-metrics:1.29.0")
    testImplementation("io.opentelemetry:opentelemetry-exporter-otlp:1.29.0")
    // Note: all org.testcontainers dependencies should have the same version
    testImplementation("org.testcontainers:testcontainers:2.0.1")
    testImplementation("org.testcontainers:testcontainers-mysql:2.0.1")
    testImplementation("org.testcontainers:testcontainers-postgresql:2.0.1")
    testImplementation("org.testcontainers:testcontainers-mariadb:2.0.1")
    testImplementation("org.testcontainers:testcontainers-junit-jupiter:2.0.1")
    testImplementation("org.testcontainers:testcontainers-toxiproxy:2.0.1")
}

tasks.test {
    filter.excludeTestsMatching("integration.*")
}

tasks.withType<Test> {
    useJUnitPlatform()
    outputs.upToDateWhen { false }
    testLogging {
        events(PASSED, FAILED, SKIPPED)
        showStandardStreams = true
        exceptionFormat = FULL
        showExceptions = true
        showCauses = true
        showStackTraces = true
    }

    reports.junitXml.required.set(true)
    reports.html.required.set(false)
}

tasks.register<Test>("test-all-mysql-aurora") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runMySQLAuroraTests")
    doFirst {
        systemProperty("test-no-docker", "true")
        systemProperty("test-no-performance", "true")
        systemProperty("test-no-pg-driver", "true")
        systemProperty("test-no-pg-engine", "true")
        systemProperty("test-no-mariadb-engine", "true")
        systemProperty("test-no-multi-az-cluster", "true")
        systemProperty("test-no-multi-az-instance", "true")
        systemProperty("test-no-aurora-limitless", "true")
        systemProperty("test-no-bg", "true")
        systemProperty("test-no-traces-telemetry", "true")
        systemProperty("test-no-metrics-telemetry", "true")
    }
}

tasks.register<Test>("test-all-mysql-aurora-ef") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runMySQLEFAuroraTests")
    doFirst {
        systemProperty("test-no-docker", "true")
        systemProperty("test-no-performance", "true")
        systemProperty("test-no-pg-driver", "true")
        systemProperty("test-no-pg-engine", "true")
        systemProperty("test-no-mariadb-engine", "true")
        systemProperty("test-no-multi-az-cluster", "true")
        systemProperty("test-no-multi-az-instance", "true")
        systemProperty("test-no-aurora-limitless", "true")
        systemProperty("test-no-bg", "true")
        systemProperty("test-no-traces-telemetry", "true")
        systemProperty("test-no-metrics-telemetry", "true")
    }
}

tasks.register<Test>("test-all-mysql-aurora-nh") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runMySQLNHAuroraTests")
    doFirst {
        systemProperty("test-no-docker", "true")
        systemProperty("test-no-performance", "true")
        systemProperty("test-no-pg-driver", "true")
        systemProperty("test-no-pg-engine", "true")
        systemProperty("test-no-mariadb-engine", "true")
        systemProperty("test-no-multi-az-cluster", "true")
        systemProperty("test-no-multi-az-instance", "true")
        systemProperty("test-no-aurora-limitless", "true")
        systemProperty("test-no-bg", "true")
        systemProperty("test-no-traces-telemetry", "true")
        systemProperty("test-no-metrics-telemetry", "true")
    }
}

tasks.register<Test>("test-all-mysql-multi-az-cluster-ef") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runMySQLEFMultiAzClusterTests")
    doFirst {
        systemProperty("test-no-docker", "true")
        systemProperty("test-no-performance", "true")
        systemProperty("test-no-pg-driver", "true")
        systemProperty("test-no-pg-engine", "true")
        systemProperty("test-no-mariadb-engine", "true")
        systemProperty("test-no-aurora", "true")
        systemProperty("test-no-multi-az-instance", "true")
        systemProperty("test-no-aurora-limitless", "true")
        systemProperty("test-no-bg", "true")
        systemProperty("test-no-traces-telemetry", "true")
        systemProperty("test-no-metrics-telemetry", "true")
    }
}

tasks.register<Test>("test-all-mysql-multi-az-cluster-nh") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runMySQLNHMultiAzClusterTests")
    doFirst {
        systemProperty("test-no-docker", "true")
        systemProperty("test-no-performance", "true")
        systemProperty("test-no-pg-driver", "true")
        systemProperty("test-no-pg-engine", "true")
        systemProperty("test-no-mariadb-engine", "true")
        systemProperty("test-no-aurora", "true")
        systemProperty("test-no-multi-az-instance", "true")
        systemProperty("test-no-aurora-limitless", "true")
        systemProperty("test-no-bg", "true")
        systemProperty("test-no-traces-telemetry", "true")
        systemProperty("test-no-metrics-telemetry", "true")
    }
}

tasks.register<Test>("test-all-mysql-multi-az-instance-ef") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runMySQLEFMultiAzInstanceTests")
    doFirst {
        systemProperty("test-no-docker", "true")
        systemProperty("test-no-performance", "true")
        systemProperty("test-no-pg-driver", "true")
        systemProperty("test-no-pg-engine", "true")
        systemProperty("test-no-mariadb-engine", "true")
        systemProperty("test-no-aurora", "true")
        systemProperty("test-no-multi-az-cluster", "true")
        systemProperty("test-no-aurora-limitless", "true")
        systemProperty("test-no-bg", "true")
        systemProperty("test-no-traces-telemetry", "true")
        systemProperty("test-no-metrics-telemetry", "true")
    }
}

tasks.register<Test>("test-all-mysql-multi-az-instance-nh") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runMySQLNHMultiAzInstanceTests")
    doFirst {
        systemProperty("test-no-docker", "true")
        systemProperty("test-no-performance", "true")
        systemProperty("test-no-pg-driver", "true")
        systemProperty("test-no-pg-engine", "true")
        systemProperty("test-no-mariadb-engine", "true")
        systemProperty("test-no-aurora", "true")
        systemProperty("test-no-multi-az-cluster", "true")
        systemProperty("test-no-aurora-limitless", "true")
        systemProperty("test-no-bg", "true")
        systemProperty("test-no-traces-telemetry", "true")
        systemProperty("test-no-metrics-telemetry", "true")
    }
}

tasks.register<Test>("test-all-pg-aurora") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runPGAuroraTests")
    doFirst {
        systemProperty("test-no-docker", "true")
        systemProperty("test-no-performance", "true")
        systemProperty("test-no-mysql-driver", "true")
        systemProperty("test-no-mysql-engine", "true")
        systemProperty("test-no-mariadb-driver", "true")
        systemProperty("test-no-mariadb-engine", "true")
        systemProperty("test-no-multi-az-cluster", "true")
        systemProperty("test-no-multi-az-instance", "true")
        systemProperty("test-no-aurora-limitless", "true")
        systemProperty("test-no-bg", "true")
        systemProperty("test-no-traces-telemetry", "true")
        systemProperty("test-no-metrics-telemetry", "true")
    }
}

tasks.register<Test>("test-all-pg-aurora-limitless") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runPGAuroraLimitlessTests")
    doFirst {
        systemProperty("test-no-docker", "true")
        systemProperty("test-no-performance", "true")
        systemProperty("test-no-failover", "true")
        systemProperty("test-no-mysql-driver", "true")
        systemProperty("test-no-mysql-engine", "true")
        systemProperty("test-no-mariadb-driver", "true")
        systemProperty("test-no-mariadb-engine", "true")
        systemProperty("test-no-aurora", "true")
        systemProperty("test-no-multi-az-cluster", "true")
        systemProperty("test-no-multi-az-instance", "true")
        systemProperty("test-no-bg", "true")
        systemProperty("test-no-traces-telemetry", "true")
        systemProperty("test-no-metrics-telemetry", "true")
    }
}


tasks.register<Test>("test-all-pg-aurora-nh") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runPGNHAuroraTests")
    doFirst {
        systemProperty("test-no-docker", "true")
        systemProperty("test-no-performance", "true")
        systemProperty("test-no-mysql-driver", "true")
        systemProperty("test-no-mysql-engine", "true")
        systemProperty("test-no-mariadb-driver", "true")
        systemProperty("test-no-mariadb-engine", "true")
        systemProperty("test-no-multi-az-cluster", "true")
        systemProperty("test-no-multi-az-instance", "true")
        systemProperty("test-no-aurora-limitless", "true")
        systemProperty("test-no-bg", "true")
        systemProperty("test-no-traces-telemetry", "true")
        systemProperty("test-no-metrics-telemetry", "true")
    }
}

tasks.register<Test>("test-all-pg-multi-az-cluster") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runPGMultiAzClusterTests")
    doFirst {
        systemProperty("test-no-docker", "true")
        systemProperty("test-no-performance", "true")
        systemProperty("test-no-mysql-driver", "true")
        systemProperty("test-no-mysql-engine", "true")
        systemProperty("test-no-mariadb-driver", "true")
        systemProperty("test-no-mariadb-engine", "true")
        systemProperty("test-no-aurora", "true")
        systemProperty("test-no-multi-az-instance", "true")
        systemProperty("test-no-aurora-limitless", "true")
        systemProperty("test-no-bg", "true")
        systemProperty("test-no-traces-telemetry", "true")
        systemProperty("test-no-metrics-telemetry", "true")
    }
}

tasks.register<Test>("test-all-pg-multi-az-cluster-nh") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runPGNHMultiAzClusterTests")
    doFirst {
        systemProperty("test-no-docker", "true")
        systemProperty("test-no-performance", "true")
        systemProperty("test-no-mysql-driver", "true")
        systemProperty("test-no-mysql-engine", "true")
        systemProperty("test-no-mariadb-driver", "true")
        systemProperty("test-no-mariadb-engine", "true")
        systemProperty("test-no-aurora", "true")
        systemProperty("test-no-multi-az-instance", "true")
        systemProperty("test-no-aurora-limitless", "true")
        systemProperty("test-no-bg", "true")
        systemProperty("test-no-traces-telemetry", "true")
        systemProperty("test-no-metrics-telemetry", "true")
    }
}

tasks.register<Test>("test-all-mysql-multi-az-cluster") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runMySQLMultiAzClusterTests")
    doFirst {
        systemProperty("test-no-docker", "true")
        systemProperty("test-no-performance", "true")
        systemProperty("test-no-pg-driver", "true")
        systemProperty("test-no-pg-engine", "true")
        systemProperty("test-no-mariadb-engine", "true")
        systemProperty("test-no-aurora", "true")
        systemProperty("test-no-multi-az-instance", "true")
        systemProperty("test-no-aurora-limitless", "true")
        systemProperty("test-no-bg", "true")
        systemProperty("test-no-traces-telemetry", "true")
        systemProperty("test-no-metrics-telemetry", "true")
    }
}

tasks.register<Test>("test-all-pg-multi-az-instance") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runPGMultiAzInstanceTests")
    doFirst {
        systemProperty("test-no-docker", "true")
        systemProperty("test-no-performance", "true")
        systemProperty("test-no-mysql-driver", "true")
        systemProperty("test-no-mysql-engine", "true")
        systemProperty("test-no-mariadb-driver", "true")
        systemProperty("test-no-mariadb-engine", "true")
        systemProperty("test-no-aurora", "true")
        systemProperty("test-no-multi-az-cluster", "true")
        systemProperty("test-no-aurora-limitless", "true")
        systemProperty("test-no-bg", "true")
        systemProperty("test-no-traces-telemetry", "true")
        systemProperty("test-no-metrics-telemetry", "true")
    }
}

tasks.register<Test>("test-all-pg-multi-az-instance-nh") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runPGNHMultiAzInstanceTests")
    doFirst {
        systemProperty("test-no-docker", "true")
        systemProperty("test-no-performance", "true")
        systemProperty("test-no-mysql-driver", "true")
        systemProperty("test-no-mysql-engine", "true")
        systemProperty("test-no-mariadb-driver", "true")
        systemProperty("test-no-mariadb-engine", "true")
        systemProperty("test-no-aurora", "true")
        systemProperty("test-no-multi-az-cluster", "true")
        systemProperty("test-no-aurora-limitless", "true")
        systemProperty("test-no-bg", "true")
        systemProperty("test-no-traces-telemetry", "true")
        systemProperty("test-no-metrics-telemetry", "true")
    }
}

tasks.register<Test>("test-all-pg-aurora-limitless-nh") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runPGNHAuroraLimitlessTests")
    doFirst {
        systemProperty("test-no-docker", "true")
        systemProperty("test-no-performance", "true")
        systemProperty("test-no-failover", "true")
        systemProperty("test-no-mysql-driver", "true")
        systemProperty("test-no-mysql-engine", "true")
        systemProperty("test-no-mariadb-driver", "true")
        systemProperty("test-no-mariadb-engine", "true")
        systemProperty("test-no-aurora", "true")
        systemProperty("test-no-multi-az-cluster", "true")
        systemProperty("test-no-multi-az-instance", "true")
        systemProperty("test-no-bg", "true")
        systemProperty("test-no-traces-telemetry", "true")
        systemProperty("test-no-metrics-telemetry", "true")
    }
}

tasks.register<Test>("test-all-mysql-multi-az-instance") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runMySQLMultiAzInstanceTests")
    doFirst {
        systemProperty("test-no-docker", "true")
        systemProperty("test-no-performance", "true")
        systemProperty("test-no-pg-driver", "true")
        systemProperty("test-no-pg-engine", "true")
        systemProperty("test-no-mariadb-engine", "true")
        systemProperty("test-no-aurora", "true")
        systemProperty("test-no-multi-az-cluster", "true")
        systemProperty("test-no-aurora-limitless", "true")
        systemProperty("test-no-bg", "true")
        systemProperty("test-no-traces-telemetry", "true")
        systemProperty("test-no-metrics-telemetry", "true")
    }
}

tasks.register<Test>("test-aurora-mysql-rw-splitting-performance") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runMySQLRWSplittingPerfTests")
    doFirst {
        systemProperty("test-no-docker", "true")
        systemProperty("test-no-multi-az-cluster", "true")
        systemProperty("test-no-multi-az-instance", "true")
        systemProperty("test-no-aurora-limitless", "true")
        systemProperty("test-no-iam", "true")
        systemProperty("test-no-secrets-manager", "true")
        systemProperty("test-no-pg-driver", "true")
        systemProperty("test-no-pg-engine", "true")
        systemProperty("test-no-mariadb-driver", "true")
        systemProperty("test-no-mariadb-engine", "true")
        systemProperty("test-no-instances-1", "true")
        systemProperty("test-no-instances-2", "true")
        systemProperty("test-no-instances-3", "true")
        systemProperty("test-no-bg", "true")
    }
}

tasks.register<Test>("test-aurora-pg-rw-splitting-performance") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runPGRWSplittingPerfTests")
    doFirst {
        systemProperty("test-no-docker", "true")
        systemProperty("test-no-multi-az-cluster", "true")
        systemProperty("test-no-multi-az-instance", "true")
        systemProperty("test-no-aurora-limitless", "true")
        systemProperty("test-no-iam", "true")
        systemProperty("test-no-secrets-manager", "true")
        systemProperty("test-no-mysql-driver", "true")
        systemProperty("test-no-mysql-engine", "true")
        systemProperty("test-no-mariadb-driver", "true")
        systemProperty("test-no-mariadb-engine", "true")
        systemProperty("test-no-instances-1", "true")
        systemProperty("test-no-instances-2", "true")
        systemProperty("test-no-instances-3", "true")
        systemProperty("test-no-bg", "true")
    }
}

tasks.register<Test>("in-container") {
    filter.excludeTestsMatching("software.*") // exclude unit tests

    // modify below filter to select specific integration tests
    // see https://docs.gradle.org/current/javadoc/org/gradle/api/tasks/testing/TestFilter.html
    filter.includeTestsMatching("integration.container.tests.*")
}
