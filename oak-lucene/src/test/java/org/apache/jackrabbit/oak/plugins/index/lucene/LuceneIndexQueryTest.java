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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import static com.google.common.collect.ImmutableList.of;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil.useV2;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests the query engine using the default index implementation: the
 * {@link LuceneIndexProvider}
 */
public class LuceneIndexQueryTest extends AbstractQueryTest {

    private Tree indexDefn;

    @Override
    protected void createTestIndexNode() throws Exception {
        Tree index = root.getTree("/");
        indexDefn = createTestIndexNode(index, LuceneIndexConstants.TYPE_LUCENE);
        useV2(indexDefn);
        indexDefn.setProperty(LuceneIndexConstants.TEST_MODE, true);
        indexDefn.setProperty(FulltextIndexConstants.EVALUATE_PATH_RESTRICTION, true);

        Tree props = TestUtil.newRulePropTree(indexDefn, "nt:base");
        props.getParent().setProperty(FulltextIndexConstants.INDEX_NODE_NAME, true);
        TestUtil.enablePropertyIndex(props, "c1/p", false);
        TestUtil.enableForFullText(props, FulltextIndexConstants.REGEX_ALL_PROPS, true);
        TestUtil.enablePropertyIndex(props, "a/name", false);
        TestUtil.enablePropertyIndex(props, "b/name", false);
        TestUtil.enableFunctionIndex(props, "length([name])");
        TestUtil.enableFunctionIndex(props, "lower([name])");
        TestUtil.enableFunctionIndex(props, "upper([name])");

        root.commit();
    }

    @Override
    protected ContentRepository createRepository() {
        return getOakRepo().createContentRepository();
    }

    Oak getOakRepo() {
        LowCostLuceneIndexProvider provider = new LowCostLuceneIndexProvider();
        return new Oak(new MemoryNodeStore(InitialContentHelper.INITIAL_CONTENT))
            .with(new OpenSecurityProvider())
            .with((QueryIndexProvider) provider)
            .with((Observer) provider)
            .with(new LuceneIndexEditorProvider());
    }

    @Test
    public void sql1() throws Exception {
        test("sql1.txt");
    }

    @Test
    public void sql2() throws Exception {
        test("sql2.txt");
    }

    @Test
    public void sql2FullText() throws Exception {
        test("sql2-fulltext.txt");
    }

    @Test
    public void ischildnodeTest() throws Exception {
        Tree tree = root.getTree("/");
        Tree parents = tree.addChild("parents");
        parents.addChild("p0").setProperty("id", "0");
        parents.addChild("p1").setProperty("id", "1");
        parents.addChild("p2").setProperty("id", "2");
        Tree children = tree.addChild("children");
        children.addChild("c1").setProperty("p", "1");
        children.addChild("c2").setProperty("p", "2");
        children.addChild("c3").setProperty("p", "3");
        children.addChild("c4").setProperty("p", "4");
        root.commit();

        Iterator<String> result = executeQuery(
            "select p.[jcr:path], p2.[jcr:path] from [nt:base] as p inner join [nt:base] as p2 on ischildnode(p2, p) where p.[jcr:path] = '/'",
            "JCR-SQL2").iterator();
        assertTrue(result.hasNext());
        assertEquals("/, /children", result.next());
        assertEquals("/, /jcr:system", result.next());
        assertEquals("/, /oak:index", result.next());
        assertEquals("/, /parents", result.next());
        assertFalse(result.hasNext());
    }

    @Test
    public void containsNot() throws Exception {

        // see also OAK-3371
        // "if we have only NOT CLAUSES we have to add a match all docs (*.*) for the
        // query to work"

        executeQuery("/jcr:root//*[jcr:contains(@a,'-test*')]", "xpath", false);

        String planPrefix = "[nt:base] as [a] /* lucene:test-index(/oak:index/test-index) ";

        assertXPathPlan("/jcr:root//*[@a]",
            planPrefix + "a:[* TO *]");

        assertXPathPlan("/jcr:root//*[jcr:contains(., '*')]",
            planPrefix + ":fulltext:* ft:(\"*\")");

        assertXPathPlan("/jcr:root//*[jcr:contains(@a,'*')]",
            planPrefix + "full:a:* ft:(a:\"*\")");

        assertXPathPlan("/jcr:root//*[jcr:contains(@a,'hello -world')]",
            planPrefix + "+full:a:hello -full:a:world ft:(a:\"hello\" -a:\"world\")");

        assertXPathPlan("/jcr:root//*[jcr:contains(@a,'test*')]",
            planPrefix + "full:a:test* ft:(a:\"test*\")");

        assertXPathPlan("/jcr:root//*[jcr:contains(@a,'-test')]",
            planPrefix + "-full:a:test *:* ft:(-a:\"test\")");

        assertXPathPlan("/jcr:root//*[jcr:contains(@a,'-test*')]",
            planPrefix + "-full:a:test* *:* ft:(-a:\"test*\")");

        assertXPathPlan("/jcr:root//*[jcr:contains(., '-*')]",
            planPrefix + "-:fulltext:* *:* ft:(-\"*\")");

        assertXPathPlan("/jcr:root//*[jcr:contains(., 'apple - pear')]",
            planPrefix + "+:fulltext:apple -:fulltext:pear ft:(\"apple\" \"-\" \"pear\")");

        assertXPathPlan("/jcr:root/content//*[jcr:contains(., 'apple - pear')]",
            planPrefix + "-:fulltext:pear +:fulltext:apple +:ancestors:/content ft:(\"apple\" \"-\" \"pear\")");

    }

    private void assertXPathPlan(String xpathQuery, String expectedPlan) {
        List<String> result = executeQuery("explain " + xpathQuery, "xpath", false);
        String plan = result.get(0);
        int newline = plan.indexOf('\n');
        if (newline >= 0) {
            plan = plan.substring(0, newline);
        }
        Assert.assertEquals(expectedPlan, plan);
    }

    @Test
    public void testRepSimilarAsNativeQuery() throws Exception {
        String nativeQueryString = "select [jcr:path] from [nt:base] where " +
            "native('lucene', 'mlt?stream.body=/test/a&mlt.fl=:path&mlt.mindf=0&mlt.mintf=0')";
        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("text", "Hello World");
        test.addChild("b").setProperty("text", "He said Hello and then the world said Hello as well.");
        test.addChild("c").setProperty("text", "He said Hi.");
        root.commit();
        Iterator<String> result = executeQuery(nativeQueryString, "JCR-SQL2").iterator();
        assertTrue(result.hasNext());
        assertEquals("/test/a", result.next());
        assertTrue(result.hasNext());
        assertEquals("/test/b", result.next());
        assertFalse(result.hasNext());
    }

    @Test
    public void testRepSimilarQuery() throws Exception {
        String query = "select [jcr:path] from [nt:base] where similar(., '/test/a')";
        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("text", "Hello World Hello World");
        test.addChild("b").setProperty("text", "Hello World");
        test.addChild("c").setProperty("text", "World");
        test.addChild("d").setProperty("text", "Hello");
        test.addChild("e").setProperty("text", "World");
        test.addChild("f").setProperty("text", "Hello");
        test.addChild("g").setProperty("text", "World");
        test.addChild("h").setProperty("text", "Hello");
        root.commit();
        Iterator<String> result = executeQuery(query, "JCR-SQL2").iterator();
        assertTrue(result.hasNext());
        assertEquals("/test/a", result.next());
        assertTrue(result.hasNext());
        assertEquals("/test/b", result.next());
        assertTrue(result.hasNext());
    }

    @Test
    public void testRepSimilarXPathQuery() throws Exception {
        String query = "//element(*, nt:base)[rep:similar(., '/test/a')]";
        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("text", "Hello World Hello World");
        test.addChild("b").setProperty("text", "Hello World");
        test.addChild("c").setProperty("text", "World");
        test.addChild("d").setProperty("text", "Hello");
        test.addChild("e").setProperty("text", "World");
        test.addChild("f").setProperty("text", "Hello");
        test.addChild("g").setProperty("text", "World");
        test.addChild("h").setProperty("text", "Hello");
        root.commit();
        Iterator<String> result = executeQuery(query, "xpath").iterator();
        assertTrue(result.hasNext());
        assertEquals("/test/a", result.next());
        assertTrue(result.hasNext());
        assertEquals("/test/b", result.next());
    }

    @Test
    public void testMultiValuedPropUpdate() throws Exception {
        Tree test = root.getTree("/").addChild("test");
        String child = "child";
        String mulValuedProp = "prop";
        test.addChild(child).setProperty(mulValuedProp, of("foo","bar"), Type.STRINGS);
        root.commit();
        assertQuery(
            "/jcr:root//*[jcr:contains(@" + mulValuedProp + ", 'foo')]",
            "xpath", of("/test/" + child));
        test.getChild(child).setProperty(mulValuedProp, new ArrayList<String>(), Type.STRINGS);
        root.commit();
        assertQuery("/jcr:root//*[jcr:contains(@" + mulValuedProp + ", 'foo')]", "xpath", new ArrayList<String>());

        test.getChild(child).setProperty(mulValuedProp, of("bar"), Type.STRINGS);
        root.commit();
        assertQuery(
            "/jcr:root//*[jcr:contains(@" + mulValuedProp + ", 'foo')]",
            "xpath", new ArrayList<String>());

        test.getChild(child).removeProperty(mulValuedProp);
        root.commit();
        assertQuery(
            "/jcr:root//*[jcr:contains(@" + mulValuedProp + ", 'foo')]",
            "xpath", new ArrayList<String>());
    }

    @SuppressWarnings("unused")
    private static void walktree(final Tree t) {
        System.out.println("+ " + t.getPath());
        for (PropertyState p : t.getProperties()) {
            System.out.println("  -" + p.getName() + "=" + p.getValue(STRING));
        }
        for (Tree t1 : t.getChildren()) {
            walktree(t1);
        }
    }

    private static Tree child(Tree t, String n, String type) {
        Tree t1 = t.addChild(n);
        t1.setProperty(JCR_PRIMARYTYPE, type, Type.NAME);
        return t1;
    }

    @Test
    public void oak3371() throws Exception {
        setTraversalEnabled(false);
        Tree t, t1;

        t = root.getTree("/");
        t = child(t, "test", NT_UNSTRUCTURED);
        t1 = child(t, "a", NT_UNSTRUCTURED);
        t1.setProperty("foo", "bar");
        t1 = child(t, "b", NT_UNSTRUCTURED);
        t1.setProperty("foo", "cat");
        t1 = child(t, "c", NT_UNSTRUCTURED);
        t1 = child(t, "d", NT_UNSTRUCTURED);
        t1.setProperty("foo", "bar cat");

        root.commit();

        assertQuery(
            "SELECT * FROM [nt:unstructured] WHERE ISDESCENDANTNODE('/test') AND CONTAINS(foo, 'bar')",
            of("/test/a", "/test/d"));

        assertQuery(
            "SELECT * FROM [nt:unstructured] WHERE ISDESCENDANTNODE('/test') AND NOT CONTAINS(foo, 'bar')",
            of("/test/b", "/test/c"));

        assertQuery(
            "SELECT * FROM [nt:unstructured] WHERE ISDESCENDANTNODE('/test') AND CONTAINS(foo, 'bar cat')",
            of("/test/d"));

        assertQuery(
            "SELECT * FROM [nt:unstructured] WHERE ISDESCENDANTNODE('/test') AND NOT CONTAINS(foo, 'bar cat')",
            of("/test/c"));

        setTraversalEnabled(true);
    }
}
