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

package integration.host;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(TestEnvironmentProvider.class)
public class TestRunner {

  @TestTemplate
  public void runMySQLAuroraTests(TestEnvironmentRequest testEnvironmentRequest) throws Exception {

    try (final TestEnvironmentConfig env = TestEnvironmentConfig.build(testEnvironmentRequest)) {
      env.runTests("mysql", "aurora");
    }
  }

  @TestTemplate
  public void runPGAuroraTests(TestEnvironmentRequest testEnvironmentRequest) throws Exception {
    try (final TestEnvironmentConfig env = TestEnvironmentConfig.build(testEnvironmentRequest)) {
      env.runTests("pg", "aurora");
    }
  }

  @TestTemplate
  public void runPGAuroraLimitlessTests(TestEnvironmentRequest testEnvironmentRequest) throws Exception {
    try (final TestEnvironmentConfig env = TestEnvironmentConfig.build(testEnvironmentRequest)) {
      env.runTests("pg", "aurora-limitless");
    }
  }

  @TestTemplate
  public void runMySQLMultiAzClusterTests(TestEnvironmentRequest testEnvironmentRequest) throws Exception {
    try (final TestEnvironmentConfig env = TestEnvironmentConfig.build(testEnvironmentRequest)) {
      env.runTests("mysql", "multi-az-cluster");
    }
  }

  @TestTemplate
  public void runPGMultiAzClusterTests(TestEnvironmentRequest testEnvironmentRequest) throws Exception {
    try (final TestEnvironmentConfig env = TestEnvironmentConfig.build(testEnvironmentRequest)) {
      env.runTests("pg", "multi-az-cluster");
    }
  }

  @TestTemplate
  public void runMySQLMultiAzInstanceTests(TestEnvironmentRequest testEnvironmentRequest) throws Exception {
    try (final TestEnvironmentConfig env = TestEnvironmentConfig.build(testEnvironmentRequest)) {
      env.runTests("mysql", "multi-az-instance");
    }
  }

  @TestTemplate
  public void runPGMultiAzInstanceTests(TestEnvironmentRequest testEnvironmentRequest) throws Exception {
    try (final TestEnvironmentConfig env = TestEnvironmentConfig.build(testEnvironmentRequest)) {
      env.runTests("pg", "multi-az-instance");
    }
  }

  @TestTemplate
  public void runMySQLEFAuroraTests(TestEnvironmentRequest testEnvironmentRequest) throws Exception {

    try (final TestEnvironmentConfig env = TestEnvironmentConfig.build(testEnvironmentRequest)) {
      env.runTests("mysql-ef", "aurora");
    }
  }

  @TestTemplate
  public void runMySQLEFMultiAzClusterTests(TestEnvironmentRequest testEnvironmentRequest) throws Exception {

    try (final TestEnvironmentConfig env = TestEnvironmentConfig.build(testEnvironmentRequest)) {
      env.runTests("mysql-ef", "multi-az-cluster");
    }
  }

  @TestTemplate
  public void runMySQLEFMultiAzInstanceTests(TestEnvironmentRequest testEnvironmentRequest) throws Exception {

    try (final TestEnvironmentConfig env = TestEnvironmentConfig.build(testEnvironmentRequest)) {
      env.runTests("mysql-ef", "multi-az-instance");
    }
  }

  @TestTemplate
  public void runMySQLNHAuroraTests(TestEnvironmentRequest testEnvironmentRequest) throws Exception {

    try (final TestEnvironmentConfig env = TestEnvironmentConfig.build(testEnvironmentRequest)) {
      env.runTests("mysql-nh", "aurora");
    }
  }

  @TestTemplate
  public void runMySQLNHMultiAzClusterTests(TestEnvironmentRequest testEnvironmentRequest) throws Exception {

    try (final TestEnvironmentConfig env = TestEnvironmentConfig.build(testEnvironmentRequest)) {
      env.runTests("mysql-nh", "multi-az-cluster");
    }
  }

  @TestTemplate
  public void runMySQLNHMultiAzInstanceTests(TestEnvironmentRequest testEnvironmentRequest) throws Exception {

    try (final TestEnvironmentConfig env = TestEnvironmentConfig.build(testEnvironmentRequest)) {
      env.runTests("mysql-nh", "multi-az-instance");
    }
  }

  @TestTemplate
  public void runPGNHAuroraTests(TestEnvironmentRequest testEnvironmentRequest) throws Exception {

    try (final TestEnvironmentConfig env = TestEnvironmentConfig.build(testEnvironmentRequest)) {
      env.runTests("pg-nh", "aurora");
    }
  }

  @TestTemplate
  public void runPGNHMultiAzClusterTests(TestEnvironmentRequest testEnvironmentRequest) throws Exception {

    try (final TestEnvironmentConfig env = TestEnvironmentConfig.build(testEnvironmentRequest)) {
      env.runTests("pg-nh", "multi-az-cluster");
    }
  }

  @TestTemplate
  public void runPGNHAuroraLimitlessTests(TestEnvironmentRequest testEnvironmentRequest) throws Exception {
    try (final TestEnvironmentConfig env = TestEnvironmentConfig.build(testEnvironmentRequest)) {
      env.runTests("pg-nh", "aurora-limitless");
    }
  }

  @TestTemplate
  public void runPGNHMultiAzInstanceTests(TestEnvironmentRequest testEnvironmentRequest) throws Exception {

    try (final TestEnvironmentConfig env = TestEnvironmentConfig.build(testEnvironmentRequest)) {
      env.runTests("pg-nh", "multi-az-instance");
    }
  }

  @TestTemplate
  public void runMySQLRWSplittingPerfTests(TestEnvironmentRequest testEnvironmentRequest) throws Exception {

    try (final TestEnvironmentConfig env = TestEnvironmentConfig.build(testEnvironmentRequest)) {
        env.runTests("mysql-rw-split-perf", "aurora");
    }
  }

  @TestTemplate
  public void runPGRWSplittingPerfTests(TestEnvironmentRequest testEnvironmentRequest) throws Exception {

    try (final TestEnvironmentConfig env = TestEnvironmentConfig.build(testEnvironmentRequest)) {
        env.runTests("pg-rw-split-perf", "aurora");
    }
  }

  @TestTemplate
  public void debugTests(TestEnvironmentRequest testEnvironmentRequest) throws Exception {

    try (final TestEnvironmentConfig env = TestEnvironmentConfig.build(testEnvironmentRequest)) {
      env.debugTests("in-container");
    }
  }
}
