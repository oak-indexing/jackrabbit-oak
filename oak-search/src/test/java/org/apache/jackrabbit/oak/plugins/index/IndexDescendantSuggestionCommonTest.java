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
package org.apache.jackrabbit.oak.plugins.index;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.commons.JcrUtils;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.IndexFormatVersion;
import org.junit.Ignore;
import org.junit.Test;

import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;
import javax.jcr.query.RowIterator;
import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;
import static org.apache.jackrabbit.JcrConstants.NT_BASE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.EVALUATE_PATH_RESTRICTION;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.INDEX_RULES;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_USE_IN_SUGGEST;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.NT_OAK_UNSTRUCTURED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class IndexDescendantSuggestionCommonTest {
    protected Repository repository = null;
    protected JackrabbitSession session = null;
    protected Node root = null;
    protected IndexOptions indexOptions;

    protected void createSuggestIndex(Node rootNode, String name, String indexedNodeType, String indexedPropertyName)
            throws Exception {
        Node def = JcrUtils.getOrAddNode(rootNode, INDEX_DEFINITIONS_NAME)
                .addNode(name, INDEX_DEFINITIONS_NODE_TYPE);
        def.setProperty(TYPE_PROPERTY_NAME, indexOptions.getIndexType());
        def.setProperty(REINDEX_PROPERTY_NAME, true);
        def.setProperty("name", name);
        def.setProperty(FulltextIndexConstants.COMPAT_MODE, IndexFormatVersion.V2.getVersion());
        def.setProperty(EVALUATE_PATH_RESTRICTION, true);

        Node propertyIdxDef = def.addNode(INDEX_RULES, JcrConstants.NT_UNSTRUCTURED)
                .addNode(indexedNodeType, JcrConstants.NT_UNSTRUCTURED)
                .addNode(FulltextIndexConstants.PROP_NODE, JcrConstants.NT_UNSTRUCTURED)
                .addNode("indexedProperty", JcrConstants.NT_UNSTRUCTURED);
        propertyIdxDef.setProperty("analyzed", true);
        propertyIdxDef.setProperty(PROP_USE_IN_SUGGEST, true);
        propertyIdxDef.setProperty("name", indexedPropertyName);
    }

    private String createSuggestQuery(String nodeTypeName, String suggestFor, String rootPath) {
        return "SELECT [rep:suggest()] as suggestion, [jcr:score] as score  FROM [" + nodeTypeName + "]" +
                " WHERE suggest('" + suggestFor + "')" +
                (rootPath == null ? "" : " AND ISDESCENDANTNODE([" + rootPath + "])");

    }

    private Set<String> getSuggestions(String query) throws Exception {
        QueryManager queryManager = session.getWorkspace().getQueryManager();
        QueryResult result = queryManager.createQuery(query, Query.JCR_SQL2).execute();
        RowIterator rows = result.getRows();

        Set<String> suggestions = newHashSet();
        while (rows.hasNext()) {
            suggestions.add(rows.nextRow().getValue("suggestion").getString());
        }

        return suggestions;
    }

    private void validateSuggestions(String query, Set<String> expected) throws Exception {
        Set<String> suggestions = getSuggestions(query);
        assertEquals("Incorrect suggestions", expected, suggestions);
    }

    //Don't break suggestions :)
    @Test
    public void noDescendantSuggestsAll() throws Exception {
        validateSuggestions(
                createSuggestQuery(NT_OAK_UNSTRUCTURED, "te", null),
                newHashSet("test1", "test2", "test3", "test4", "test5", "test6"));
    }

    //OAK-3994
    @Test
    public void rootIndexWithDescendantConstraint() throws Exception {
        validateSuggestions(
                createSuggestQuery(NT_OAK_UNSTRUCTURED, "te", "/content1"),
                newHashSet("test2", "test3"));
    }

    //OAK-3994
    @Test
    public void descendantSuggestionRequirePathRestrictionIndex() throws Exception {
        Node rootIndexDef = root.getNode("oak:index/sugg-idx");
        rootIndexDef.getProperty(EVALUATE_PATH_RESTRICTION).remove();
        rootIndexDef.setProperty(REINDEX_PROPERTY_NAME, true);
        session.save();

        //Without path restriction indexing, descendant clause shouldn't be respected
        validateSuggestions(
                createSuggestQuery(NT_OAK_UNSTRUCTURED, "te", "/content1"),
                newHashSet("test1", "test2", "test3", "test4", "test5", "test6"));
    }

    @Ignore("OAK-3992")
    @Test
    public void ambiguousSubtreeIndexWithDescendantConstraint() throws Exception {
        String query = createSuggestQuery(NT_OAK_UNSTRUCTURED, "te", "/content2");
        String explainQuery = "EXPLAIN " + createSuggestQuery(NT_OAK_UNSTRUCTURED, "te", "/content2");

        QueryManager queryManager = session.getWorkspace().getQueryManager();
        QueryResult result = queryManager.createQuery(explainQuery, Query.JCR_SQL2).execute();
        RowIterator rows = result.getRows();

        String explanation = rows.nextRow().toString();
        assertTrue("Subtree index should get picked", explanation.contains("lucene:sugg-idx(/content2/oak:index/sugg-idx)"));

        validateSuggestions(query, newHashSet("test4"));
    }

    //OAK-3994
    @Test
    public void unambiguousSubtreeIndexWithDescendantConstraint() throws Exception {
        validateSuggestions(
                createSuggestQuery(NT_BASE, "te", "/content3"),
                newHashSet("test5", "test6"));
    }

    //OAK-3994
    @Test
    public void unambiguousSubtreeIndexWithSubDescendantConstraint() throws Exception {
        validateSuggestions(
                createSuggestQuery(NT_BASE, "te", "/content3/sC"),
                newHashSet("test6"));
    }

    @Ignore("OAK-3993")
    @Test
    public void unionOnTwoDescendants() throws Exception {
        validateSuggestions(
                createSuggestQuery(NT_OAK_UNSTRUCTURED, "te", "/content1") +
                        " UNION " +
                        createSuggestQuery(NT_BASE, "te", "/content3"),
                newHashSet("test2", "test3", "test5"));
    }
}
