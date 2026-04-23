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
package com.pinterest.secor.uploader;

import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.common.SecorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Manages uploads to Microsoft Azure blob storage using Azure Storage SDK v12.
 * Supports both shared key authentication and managed identity (via DefaultAzureCredential).
 *
 * If secor.azure.account.key is set, shared key authentication is used.
 * Otherwise, DefaultAzureCredential is used, which supports managed identity,
 * environment variables, Azure CLI, and other credential sources.
 *
 * @author Taichi Nakashima (nsd22843@gmail.com)
 */
public class AzureUploadManager extends UploadManager {
    private static final Logger LOG = LoggerFactory.getLogger(AzureUploadManager.class);
    private static final ExecutorService executor = Executors.newFixedThreadPool(256);

    private BlobServiceClient blobServiceClient;

    public AzureUploadManager(SecorConfig config) throws Exception {
        super(config);

        String accountName = mConfig.getAzureAccountName();
        String accountKey = mConfig.getAzureAccountKey();
        String protocol = mConfig.getAzureEndpointsProtocol();
        String endpoint = protocol + "://" + accountName + ".blob.core.windows.net";

        if (accountKey != null && !accountKey.isEmpty()) {
            LOG.info("Using shared key authentication for Azure Blob Storage");
            StorageSharedKeyCredential credential = new StorageSharedKeyCredential(accountName, accountKey);
            blobServiceClient = new BlobServiceClientBuilder()
                    .endpoint(endpoint)
                    .credential(credential)
                    .buildClient();
        } else {
            LOG.info("Using DefaultAzureCredential (managed identity) for Azure Blob Storage");
            blobServiceClient = new BlobServiceClientBuilder()
                    .endpoint(endpoint)
                    .credential(new DefaultAzureCredentialBuilder().build())
                    .buildClient();
        }
    }

    @Override
    public Handle<?> upload(LogFilePath localPath) throws Exception {
        final String azureContainer = mConfig.getAzureContainer();
        final String azureKey = localPath.withPrefix(mConfig.getAzurePath(), mConfig).getLogFilePath();
        final File localFile = new File(localPath.getLogFilePath());

        LOG.info("uploading file {} to azure://{}/{}", localFile, azureContainer, azureKey);
        final Future<?> f = executor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(azureContainer);
                    if (!containerClient.exists()) {
                        containerClient.create();
                    }

                    BlobClient blobClient = containerClient.getBlobClient(azureKey);
                    blobClient.upload(new FileInputStream(localFile), localFile.length(), true);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });

        return new FutureHandle(f);
    }
}
