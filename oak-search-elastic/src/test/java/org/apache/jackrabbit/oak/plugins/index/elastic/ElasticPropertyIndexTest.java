/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.elastic;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;

import static java.util.Collections.singletonList;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROPDEF_PROP_NODE_NAME;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class ElasticPropertyIndexTest extends ElasticAbstractQueryTest {

    @Test
    public void testBulkProcessorFlushLimit() throws Exception {
        setIndex("test1", createIndex("propa"));

        Tree test = root.getTree("/").addChild("test");
        for (int i = 1; i < 249; i++) {
            test.addChild("a" + i).setProperty("propa", "foo" + i);
        }
        root.commit();

        // 250 is the default flush limit for bulk processor, and we added just less than 250 nodes
        // So once the index writer is closed , bulk Processor would be closed and all the 248 entries should be flushed.
        // Make sure that the last entry is indexed correctly.
        String propaQuery = "select [jcr:path] from [nt:base] where [propa] = 'foo248'";
        assertEventually(() -> {
            assertThat(explain(propaQuery), containsString("elasticsearch:test1"));

            assertQuery(propaQuery, singletonList("/test/a248"));
        });

        // Now we test for 250 < nodes < 500

        for (int i = 250 ; i < 300 ; i ++) {
            test.addChild("a" + i).setProperty("propa", "foo" + i);
        }
        root.commit();
        String propaQuery2 = "select [jcr:path] from [nt:base] where [propa] = 'foo299'";
        assertEventually(() -> {
            assertThat(explain(propaQuery2), containsString("elasticsearch:test1"));

            assertQuery(propaQuery2, singletonList("/test/a299"));
        });
    }

    @Test
    public void indexSelection() throws Exception {
        setIndex("test1", createIndex("propa", "propb"));
        setIndex("test2", createIndex("propc"));

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("propa", "foo");
        test.addChild("b").setProperty("propa", "foo");
        test.addChild("c").setProperty("propa", "foo2");
        test.addChild("d").setProperty("propc", "foo");
        test.addChild("e").setProperty("propd", "foo");
        root.commit();

        String propaQuery = "select [jcr:path] from [nt:base] where [propa] = 'foo'";

        assertEventually(() -> {
            assertThat(explain(propaQuery), containsString("elasticsearch:test1"));
            assertThat(explain("select [jcr:path] from [nt:base] where [propc] = 'foo'"), containsString("elasticsearch:test2"));

            assertQuery(propaQuery, Arrays.asList("/test/a", "/test/b"));
            assertQuery("select [jcr:path] from [nt:base] where [propa] = 'foo2'", singletonList("/test/c"));
            assertQuery("select [jcr:path] from [nt:base] where [propc] = 'foo'", singletonList("/test/d"));
        });
    }

    //OAK-3825
    @Test
    public void nodeNameViaPropDefinition() throws Exception {
        //make index
        IndexDefinitionBuilder builder = createIndex();
        builder.includedPaths("/test")
                .indexRule("nt:base")
                .property("nodeName", PROPDEF_PROP_NODE_NAME);
        setIndex("test1", builder);
        root.commit();

        //add content
        Tree test = root.getTree("/").addChild("test");
        test.addChild("foo");
        test.addChild("camelCase");
        test.addChild("sc").addChild("bar");
        root.commit();

        String queryPrefix = "select [jcr:path] from [nt:base] where ISDESCENDANTNODE('/test') AND ";
        //test
        String propabQuery = queryPrefix + "LOCALNAME() = 'foo'";

        assertEventually(() -> {
            String explanation = explain(propabQuery);
            assertThat(explanation, containsString("elasticsearch:test1(/oak:index/test1) "));
            assertThat(explanation, containsString("{\"term\":{\":nodeName\":{\"value\":\"foo\","));
            assertQuery(propabQuery, singletonList("/test/foo"));

            assertQuery(queryPrefix + "LOCALNAME() = 'bar'", singletonList("/test/sc/bar"));
            assertQuery(queryPrefix + "LOCALNAME() LIKE 'foo'", singletonList("/test/foo"));
            assertQuery(queryPrefix + "LOCALNAME() LIKE 'camel%'", singletonList("/test/camelCase"));

            assertQuery(queryPrefix + "NAME() = 'bar'", singletonList("/test/sc/bar"));
            assertQuery(queryPrefix + "NAME() LIKE 'foo'", singletonList("/test/foo"));
            assertQuery(queryPrefix + "NAME() LIKE 'camel%'", singletonList("/test/camelCase"));
        });
    }

    @Test
    public void emptyIndex() throws Exception {
        setIndex("test1", createIndex("propa", "propb"));
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a");
        test.addChild("b");
        root.commit();

        assertEventually(() -> assertThat(explain("select [jcr:path] from [nt:base] where [propa] = 'foo'"),
                containsString("elasticsearch:test1")));
    }

    @Test
    public void propertyExistenceQuery() throws Exception {
        setIndex("test1", createIndex("propa", "propb"));

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("propa", "a");
        test.addChild("b").setProperty("propa", "c");
        test.addChild("c").setProperty("propb", "e");
        root.commit();

        assertEventually(() -> assertQuery("select [jcr:path] from [nt:base] where propa is not null",
                Arrays.asList("/test/a", "/test/b")));
    }


    @Test
    public void testSim() throws Exception {
        IndexDefinitionBuilder builder = createIndex("fv");
        builder.indexRule("nt:base").property("fv").useInSimilarity(true).nodeScopeIndex();
        Tree index = setIndex("test1", builder);
        root.commit();
        Tree test = root.getTree("/").addChild("test");

        URI uri = getClass().getResource("/org/apache/jackrabbit/oak/query/fvs.csv").toURI();
        File file = new File(uri);

        Collection<String> children = new LinkedList<>();
        for (String line : IOUtils.readLines(new FileInputStream(file), Charset.defaultCharset())) {
            String[] split = line.split(",");
            List<Double> values = new LinkedList<>();
            int i = 0;
            for (String s : split) {
                if (i > 0) {
                    values.add(Double.parseDouble(s));
                }
                i++;
            }

            byte[] bytes = toByteArray(values);
            List<Double> actual = toDoubles(bytes);
            assertEquals(values, actual);

            Blob blob = root.createBlob(new ByteArrayInputStream(bytes));
            String name = split[0];
            Tree child = test.addChild(name);
            child.setProperty("fv", blob, Type.BINARY);
            children.add(child.getPath());
        }
        root.commit();

        // check that similarity changes across different feature vectors
        List<String> baseline = new LinkedList<>();
        for (String similarPath : children) {
            String query = "select [jcr:path] from [nt:base] where similar(., '" + similarPath + "')";

            Iterator<String> result = executeQuery(query, "JCR-SQL2", false, true).iterator();
            List<String> current = new LinkedList<>();
            while (result.hasNext()) {
                String next = result.next();
                current.add(next);
            }
            assertNotEquals(baseline, current);
            baseline.clear();
            baseline.addAll(current);
        }

        Thread.sleep(10000);
    }

    private static byte[] toByteArray(List<Double> values) {
        int blockSize = Double.SIZE / Byte.SIZE;
        byte[] bytes = new byte[values.size() * blockSize];
        for (int i = 0, j = 0; i < values.size(); i++, j += blockSize) {
            ByteBuffer.wrap(bytes, j, blockSize).putDouble(values.get(i));
        }
        return bytes;
    }

    private static List<Double> toDoubles(byte[] array) {
        int blockSize = Double.SIZE / Byte.SIZE;
        ByteBuffer wrap = ByteBuffer.wrap(array);
        int capacity = array.length / blockSize;
        List<Double> doubles = new ArrayList<>(capacity);
        for (int i = 0; i < capacity; i++) {
            double e = wrap.getDouble(i * blockSize);
            doubles.add(e);
        }
        return doubles;
    }

}
