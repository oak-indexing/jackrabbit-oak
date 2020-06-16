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
package org.apache.jackrabbit.oak.plugins.blob;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.util.Map;

import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.h2.mvstore.MVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A cache that internally uses a key-value store (an MVStore).
 */
public class MVStoreCache {

    private static final String FILE_NAME = "mvstoreCache.db";
    private static final String MAP_NAME = "dataStoreCache";

    private static final Logger LOG = LoggerFactory.getLogger(MVStoreCache.class);

    private final MVStore store;
    private final Map<String, byte[]> map;

    public static MVStoreCache open(String directory) {
        File f = new File(directory, FILE_NAME);
        if (!f.exists()) {
            return new MVStoreCache(null);
        }
        LOG.info("Opening {}", f.getAbsolutePath());
        MVStore store = MVStore.open(f.getAbsolutePath());
        return new MVStoreCache(store);
    }

    private MVStoreCache(MVStore store) {
        this.store = store;
        if (store != null) {
            map = store.openMap(MAP_NAME);
        } else {
            map = null;
        }
    }

    public DataRecord readIfExists(DataIdentifier identifier) {
        if (store == null) {
            return null;
        }
        String key = identifier.toString();
        byte[] bytes = map.get(key);
        if (bytes == null) {
            return null;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Found {}: {} bytes", key, bytes.length);
        }
        return new InMemoryDataRecord(identifier, bytes);
    }

    public void close() {
        if (store != null) {
            store.close();
        }
    }

    private static class InMemoryDataRecord implements DataRecord {

        private final DataIdentifier identifier;
        private final byte[] bytes;

        InMemoryDataRecord(DataIdentifier identifier, byte[] bytes) {
            this.identifier = identifier;
            this.bytes = bytes;
        }

        @Override
        public DataIdentifier getIdentifier() {
            return identifier;
        }

        @Override
        public long getLastModified() {
            return 0;
        }

        @Override
        public long getLength() throws DataStoreException {
            return bytes.length;
        }

        @Override
        public String getReference() {
            return null;
        }

        @Override
        public InputStream getStream() throws DataStoreException {
            return new ByteArrayInputStream(bytes);
        }

    }

}
