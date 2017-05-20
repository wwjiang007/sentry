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
package org.apache.sentry.core.common.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.shiro.config.Ini;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.ByteStreams;
import com.google.common.io.Resources;

public final class PolicyFiles {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(PolicyFiles.class);

  public static void copyToDir(File dest, String... resources)
      throws FileNotFoundException, IOException {
    for(String resource : resources) {
      LOGGER.debug("Copying " + resource + " to " + dest);
      try (OutputStream output = Files.newOutputStream(new File(dest, resource).toPath())) {
        Resources.copy(Resources.getResource(resource), output);
      }
    }
  }

  public static void copyToDir(FileSystem fs, Path dest, String... resources)
      throws FileNotFoundException, IOException {
    for(String resource : resources) {
      InputStream in = Resources.getResource(resource).openStream();
      FSDataOutputStream out = fs.create(new Path(dest, resource));
      long bytes = ByteStreams.copy(in, out);
      in.close();
      out.hflush();
      out.close();
      LOGGER.debug("Copying " + resource + " to " + dest + ", bytes " + bytes);
    }
  }

  public static void copyFilesToDir(FileSystem fs, Path dest, File inputFile)
      throws IOException {
    try (InputStream input = Files.newInputStream(inputFile.toPath());
      FSDataOutputStream out = fs.create(new Path(dest, inputFile.getName()))) {
      ByteStreams.copy(input, out);
      out.hflush();
      out.close();
    }
  }


  public static Ini loadFromPath(FileSystem fileSystem, Path path) throws IOException {
    InputStream inputStream = null;
    try {
      LOGGER.debug("Opening " + path);
      inputStream = fileSystem.open(path);
      Ini ini = new Ini();
      ini.load(inputStream);
      return ini;
    } finally {
      if(inputStream != null) {
        try {
          inputStream.close();
        } catch (IOException e) {
          LOGGER.warn("Error closing " + inputStream);
        }
      }
    }
  }

  private PolicyFiles() {
    // Make constructor private to avoid instantiation
  }
}
