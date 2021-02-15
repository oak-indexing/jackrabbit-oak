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

import static org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil.useV2;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.List;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
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
    public void containsNot() {

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
}
