/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sentry.provider.db.log.appender;

import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.Writer;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.helpers.CountingQuietWriter;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.helpers.OptionConverter;
import org.apache.log4j.spi.LoggingEvent;

public class RollingFileWithoutDeleteAppender extends FileAppender {
  /**
   * The default maximum file size is 10MB.
   */
  protected long maxFileSize = 10 * 1024 * 1024;

  private long nextRollover = 0;

  /**
   * The default constructor simply calls its {@link FileAppender#FileAppender
   * parents constructor}.
   */
  public RollingFileWithoutDeleteAppender() {
    super();
  }

  /**
   * Instantiate a RollingFileAppender and open the file designated by
   * <code>filename</code>. The opened filename will become the ouput
   * destination for this appender.
   * <p>
   * If the <code>append</code> parameter is true, the file will be appended to.
   * Otherwise, the file desginated by <code>filename</code> will be truncated
   * before being opened.
   */
  public RollingFileWithoutDeleteAppender(Layout layout, String filename,
      boolean append) throws IOException {
    super(layout, getLogFileName(filename), append);
  }

  /**
   * Instantiate a FileAppender and open the file designated by
   * <code>filename</code>. The opened filename will become the output
   * destination for this appender.
   * <p>
   * The file will be appended to.
   */
  public RollingFileWithoutDeleteAppender(Layout layout, String filename)
      throws IOException {
    super(layout, getLogFileName(filename));
  }

  /**
   * Get the maximum size that the output file is allowed to reach before being
   * rolled over to backup files.
   */
  public long getMaximumFileSize() {
    return maxFileSize;
  }

  /**
   * Implements the usual roll over behaviour.
   * <p>
   * <code>File</code> is renamed <code>File.yyyyMMddHHmmss</code> and closed. A
   * new <code>File</code> is created to receive further log output.
   */
  // synchronization not necessary since doAppend is alreasy synched
  public void rollOver() {
    if (qw != null) {
      long size = ((CountingQuietWriter) qw).getCount();
      LogLog.debug("rolling over count=" + size);
      // if operation fails, do not roll again until
      // maxFileSize more bytes are written
      nextRollover = size + maxFileSize;
    }

    this.closeFile(); // keep windows happy.

    String newFileName = getLogFileName(fileName);
    try {
      // This will also close the file. This is OK since multiple
      // close operations are safe.
      this.setFile(newFileName, false, bufferedIO, bufferSize);
      nextRollover = 0;
    } catch (IOException e) {
      if (e instanceof InterruptedIOException) {
        Thread.currentThread().interrupt();
      }
      LogLog.error("setFile(" + newFileName + ", false) call failed: "  + e.getMessage(), e);
    }
  }

  public synchronized void setFile(String fileName, boolean append,
      boolean bufferedIO, int bufferSize) throws IOException {
    super.setFile(fileName, append, this.bufferedIO, this.bufferSize);
    if (append) {
      File f = new File(fileName);
      ((CountingQuietWriter) qw).setCount(f.length());
    }
  }

  /**
   * Set the maximum size that the output file is allowed to reach before being
   * rolled over to backup files.
   * <p>
   * This method is equivalent to {@link #setMaxFileSize} except that it is
   * required for differentiating the setter taking a <code>long</code> argument
   * from the setter taking a <code>String</code> argument by the JavaBeans
   * {@link java.beans.Introspector Introspector}.
   *
   * @see #setMaxFileSize(String)
   */
  public void setMaximumFileSize(long maxFileSize) {
    this.maxFileSize = maxFileSize;
  }

  /**
   * Set the maximum size that the output file is allowed to reach before being
   * rolled over to backup files.
   * <p>
   * In configuration files, the <b>MaxFileSize</b> option takes an long integer
   * in the range 0 - 2^63. You can specify the value with the suffixes "KB",
   * "MB" or "GB" so that the integer is interpreted being expressed
   * respectively in kilobytes, megabytes or gigabytes. For example, the value
   * "10KB" will be interpreted as 10240.
   */
  public void setMaxFileSize(String value) {
    maxFileSize = OptionConverter.toFileSize(value, maxFileSize + 1);
  }

  protected void setQWForFiles(Writer writer) {
    this.qw = new CountingQuietWriter(writer, errorHandler);
  }

  /**
   * This method differentiates RollingFileAppender from its super class.
   */
  protected void subAppend(LoggingEvent event) {
    super.subAppend(event);

    if (fileName != null && qw != null) {
      long size = ((CountingQuietWriter) qw).getCount();
      if (size >= maxFileSize && size >= nextRollover) {
        rollOver();
      }
    }
  }

  // Mangled file name. Append the current timestamp
  private static String getLogFileName(String oldFileName) {
    return oldFileName + "." + Long.toString(System.currentTimeMillis());
  }
}
