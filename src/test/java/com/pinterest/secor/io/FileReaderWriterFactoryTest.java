/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.pinterest.secor.io;

import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.util.ReflectionUtil;
import junit.framework.TestCase;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.DefaultCodec;

import java.io.BufferedWriter;
import java.io.File;
import java.io.OutputStreamWriter;

/**
 * Test the file readers and writers
 *
 * @author Praveen Murugesan (praveen@uber.com)
 */


public class FileReaderWriterFactoryTest extends TestCase {

    private static final String DIR = "/some_parent_dir/some_topic/some_partition/some_other_partition";
    private static final String BASENAME = "10_0_00000000000000000100";
    private static final String PATH = DIR + "/" + BASENAME;
    private static final String PATH_GZ = DIR + "/" + BASENAME + ".json";

    private LogFilePath mLogFilePath;
    private LogFilePath mLogFilePathGz;
    private SecorConfig mConfig;
    private MiniDFSCluster miniDFSCluster;
    private FileSystem fs;
    private Configuration hadoopConf;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        hadoopConf = new Configuration();
        File baseDir = new File("./target/hdfs/test-cluster").getAbsoluteFile();
        org.apache.commons.io.FileUtils.deleteDirectory(baseDir); // Clean up before start
        hadoopConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
        hadoopConf.set("io.compression.codecs", "org.apache.hadoop.io.compress.DefaultCodec");
        hadoopConf.set("mapreduce.output.fileoutputformat.compress", "true");
        hadoopConf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.DefaultCodec");
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(hadoopConf);
        miniDFSCluster = builder.build();
        fs = miniDFSCluster.getFileSystem();
        mLogFilePath = new LogFilePath("hdfs:/", fs.getUri() + PATH);
        mLogFilePathGz = new LogFilePath("hdfs:/", fs.getUri() + PATH_GZ);
    }

    @Override
    public void tearDown() throws Exception {
        if (miniDFSCluster != null) {
            miniDFSCluster.shutdown();
        }
        super.tearDown();
    }

    private void setupSequenceFileReaderConfig() {
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.addProperty("secor.file.reader.writer.factory",
                "com.pinterest.secor.io.impl.SequenceFileReaderWriterFactory");
        mConfig = new SecorConfig(properties);
    }

    private void setupDelimitedTextFileWriterConfig() {
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.addProperty("secor.file.reader.writer.factory",
                "com.pinterest.secor.io.impl.DelimitedTextFileReaderWriterFactory");
        mConfig = new SecorConfig(properties);
    }

    public void testSequenceFileReader() throws Exception {
        setupSequenceFileReaderConfig();
        // Write a SequenceFile to HDFS
        Path fsPath = new Path(fs.getUri() + PATH);
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, hadoopConf, fsPath, LongWritable.class, BytesWritable.class);
        writer.append(new LongWritable(1L), new BytesWritable(new byte[]{1,2,3}));
        writer.close();
        // Read using ReflectionUtil
        ReflectionUtil.createFileReader(mConfig.getFileReaderWriterFactory(), mLogFilePath, null, mConfig);

        // Compressed
//        Path fsPathGz = new Path(PATH_GZ);
//        SequenceFile.Writer writerGz = SequenceFile.createWriter(fs, hadoopConf, fsPathGz, LongWritable.class, BytesWritable.class, SequenceFile.CompressionType.BLOCK, new GzipCodec());
//        writerGz.append(new LongWritable(2L), new BytesWritable(new byte[]{4,5,6}));
//        writerGz.close();
//        ReflectionUtil.createFileWriter(mConfig.getFileReaderWriterFactory(), mLogFilePathGz, new GzipCodec(), mConfig);
    }

    public void testSequenceFileWriter() throws Exception {
        setupSequenceFileReaderConfig();
        FileWriter writer = ReflectionUtil.createFileWriter(mConfig.getFileReaderWriterFactory(), mLogFilePath, null, mConfig);
        assert writer.getLength() == 138L;
        writer.close();
        writer = ReflectionUtil.createFileWriter(mConfig.getFileReaderWriterFactory(), mLogFilePathGz, new DefaultCodec(), mConfig);
        assert writer.getLength() == 138L;
        writer.close();
    }

    public void testDelimitedTextFileWriter() throws Exception {
        setupDelimitedTextFileWriterConfig();
        FileWriter writer = (FileWriter) ReflectionUtil.createFileWriter(mConfig.getFileReaderWriterFactory(), mLogFilePath, null, mConfig);
        assert writer.getLength() == 0L;
        writer.close();
        writer = (FileWriter) ReflectionUtil.createFileWriter(mConfig.getFileReaderWriterFactory(), mLogFilePathGz, null, mConfig);
        assert writer.getLength() == 0L;
        writer.close();
    }

    public void testDelimitedTextFileReader() throws Exception {
        setupDelimitedTextFileWriterConfig();
        Path filePath = fs.makeQualified(new Path(fs.getUri() + PATH));

        // Open output stream to write
        try (FSDataOutputStream out = fs.create(filePath);
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"))) {

            // Write delimited lines (e.g., tab-separated)
            writer.write("id\tname\tage");
            writer.newLine();
            writer.write("1\tAlice\t30");
            writer.newLine();
            writer.write("2\tBob\t28");
            writer.newLine();
        }
        ReflectionUtil.createFileReader(mConfig.getFileReaderWriterFactory(), mLogFilePath, null, mConfig);
        //ReflectionUtil.createFileReader(mConfig.getFileReaderWriterFactory(), mLogFilePathGz, null, mConfig);
    }
}