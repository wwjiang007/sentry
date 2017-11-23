/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.policy.solr;

import java.io.File;
import java.io.IOException;

import org.junit.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.sentry.core.common.utils.PolicyFiles;

public class TestSolrPolicyEngineLocalFS extends AbstractTestSolrPolicyEngine {

  @Override
  protected void  afterSetup() throws IOException {
    File baseDir = getBaseDir();
    Assert.assertNotNull(baseDir);
    Assert.assertTrue(baseDir.isDirectory() || baseDir.mkdirs());
    PolicyFiles.copyToDir(baseDir, "solr-policy-test-authz-provider.ini");
    setPolicy(SolrPolicyTestUtil.createPolicyEngineForTest(new File(baseDir, "solr-policy-test-authz-provider.ini").getPath()));
  }
  @Override
  protected void beforeTeardown() throws IOException {
    File baseDir = getBaseDir();
    Assert.assertNotNull(baseDir);
    FileUtils.deleteQuietly(baseDir);
  }
}
