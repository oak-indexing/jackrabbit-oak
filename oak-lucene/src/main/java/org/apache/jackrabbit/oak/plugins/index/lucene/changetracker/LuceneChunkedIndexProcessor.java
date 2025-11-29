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
package org.apache.jackrabbit.oak.plugins.index.lucene.changetracker;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.lucene.FieldFactory;
import org.apache.jackrabbit.oak.plugins.index.search.Aggregate;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.changetracker.ChangeEntry;
import org.apache.jackrabbit.oak.plugins.index.search.changetracker.IndexProgressMetadata;
import org.apache.jackrabbit.oak.plugins.index.search.changetracker.IndexProgressMetadataManager;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.LuceneIndexWriter;
import org.apache.jackrabbit.oak.spi.filter.PathFilter;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.PropertyType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Lucene-specific implementation of chunked index processing.
 * 
 * <p>This class processes changes from the change tracking index in chunks,
 * reading current content from NodeStore and indexing it to Lucene.
 * 
 * <p><strong>Key Features:</strong>
 * <ul>
 *   <li>Queries change tracking index for unprocessed changed paths</li>
 *   <li>Reads current content from NodeStore (not historical checkpoint content)</li>
 *   <li>Processes in configurable chunk sizes</li>
 *   <li>Creates Lucene documents directly (bypassing package-private LuceneIndexEditor)</li>
 *   <li>Tracks progress to enable resumption after failures</li>
 *   <li>Uses Oak's {@link Aggregate} API to detect and handle aggregation re-indexing</li>
 * </ul>
 * 
 * <p><strong>Current Limitations:</strong>
 * <ul>
 *   <li>Document creation is simplified - needs integration with Oak's IndexingRule and PropertyDefinition APIs</li>
 *   <li>Doesn't yet handle relative properties (e.g., jcr:content/jcr:data) - see {@link #createLuceneDocument}</li>
 *   <li>Doesn't yet respect all property configurations (analyzed, stored, boost, etc.)</li>
 *   <li>See OAK_API_INTEGRATION_TODO.md for detailed plan to integrate with Oak's indexing APIs</li>
 * </ul>
 */
public class LuceneChunkedIndexProcessor {
    
    private static final Logger LOG = LoggerFactory.getLogger(LuceneChunkedIndexProcessor.class);
    
    private static final int DEFAULT_CHUNK_SIZE = 10000;
    private static final String CHUNK_SIZE_PROPERTY = "oak.changeTracker.chunkSize";
    
    /**
     * Error rate threshold (percentage) above which a warning is logged.
     * If more than this percentage of entries fail, system health may be degraded.
     */
    private static final double ERROR_RATE_WARNING_THRESHOLD = 5.0;  // 5%
    
    /**
     * Circuit breaker threshold (percentage). If error rate exceeds this, processing stops.
     * This prevents continued processing when the system is severely degraded.
     */
    private static final double ERROR_RATE_CIRCUIT_BREAKER_THRESHOLD = 25.0;  // 25%
    
    private final NodeStore nodeStore;
    // private final IndexReader changeTrackingReader;
    private final IndexProgressMetadataManager metadataManager;
    private final int chunkSize;
    private final ChangeTrackingIndexQuery changeTrackingQuery;
    
    /**
     * Tracks failed entries per index for retry.
     * Map: indexPath -> List of failed ChangeEntry objects
     */
    private final Map<String, List<FailedEntry>> failedEntriesMap = new HashMap<>();
    
    /**
     * Tracks error statistics per index.
     * Map: indexPath -> ErrorStatistics
     */
    private final Map<String, ErrorStatistics> errorStatsMap = new HashMap<>();
    
    /**
     * Represents a failed entry with error details for retry.
     */
    private static class FailedEntry {
        final ChangeEntry entry;
        // final String errorMessage;
        // final long failureTimestamp;
        int retryCount;
        
        FailedEntry(ChangeEntry entry, String errorMessage) {
            this.entry = entry;
            // this.errorMessage = errorMessage;
            // this.failureTimestamp = System.currentTimeMillis();
            this.retryCount = 0;
        }
    }
    
    /**
     * Tracks error statistics for an index.
     */
    private static class ErrorStatistics {
        long totalProcessed = 0;
        long totalErrors = 0;
        long consecutiveErrors = 0;
        // long lastErrorTime = 0;
        
        double getErrorRate() {
            return totalProcessed > 0 ? (totalErrors * 100.0 / totalProcessed) : 0.0;
        }
        
        void recordSuccess() {
            totalProcessed++;
            consecutiveErrors = 0;
        }
        
        void recordError() {
            totalProcessed++;
            totalErrors++;
            consecutiveErrors++;
            // lastErrorTime = System.currentTimeMillis();
        }
        
        boolean shouldTriggerCircuitBreaker() {
            return getErrorRate() > ERROR_RATE_CIRCUIT_BREAKER_THRESHOLD;
        }
        
        boolean shouldWarn() {
            return getErrorRate() > ERROR_RATE_WARNING_THRESHOLD;
        }
    }
    
    /**
     * Creates a Lucene chunked index processor.
     * 
     * @param nodeStore the node store for reading current state
     * @param changeTrackingReader the Lucene reader for the change tracking index
     * @param metadataManager the metadata manager for progress tracking
     */
    public LuceneChunkedIndexProcessor(@NotNull NodeStore nodeStore,
                                       @NotNull IndexReader changeTrackingReader,
                                       @NotNull IndexProgressMetadataManager metadataManager) {
        this(nodeStore, changeTrackingReader, metadataManager,
             Integer.getInteger(CHUNK_SIZE_PROPERTY, DEFAULT_CHUNK_SIZE));
    }
    
    /**
     * Creates a Lucene chunked index processor with custom chunk size.
     */
    public LuceneChunkedIndexProcessor(@NotNull NodeStore nodeStore,
                                       @NotNull IndexReader changeTrackingReader,
                                       @NotNull IndexProgressMetadataManager metadataManager,
                                       int chunkSize) {
        this.nodeStore = nodeStore;
        // this.changeTrackingReader = changeTrackingReader;
        this.metadataManager = metadataManager;
        this.chunkSize = chunkSize;
        this.changeTrackingQuery = new ChangeTrackingIndexQuery(changeTrackingReader);
        LOG.info("LuceneChunkedIndexProcessor initialized with chunk size: {}", chunkSize);
    }
    
    /**
     * Processes a chunk of changes for the given index.
     * 
     * <p><strong>Production Implementation:</strong> This method queries the change tracking index
     * for unprocessed changes and indexes them directly using Lucene APIs. It bypasses the
     * LuceneIndexEditor (which is package-private) and writes documents directly to the index writer.
     * 
     * @param indexPath the path of the index definition
     * @param indexDefinition the index definition
     * @param indexWriter the Lucene index writer
     * @return the number of changes processed in this chunk
     * @throws IOException if reading or writing fails
     * @throws CommitFailedException if committing changes fails
     */
    public int processChangesChunk(@NotNull String indexPath,
                                     @NotNull IndexDefinition indexDefinition,
                                     @NotNull LuceneIndexWriter indexWriter)
            throws IOException, CommitFailedException {
        
        // Get current progress
        IndexProgressMetadata progress = metadataManager.getIndexProgress(indexPath);
        long lastTimestamp = progress.getLastProcessedTimestamp();
        long lastSerial = progress.getLastProcessedSerialNumber();
        
        // Query change tracking index for next chunk of changes using cached query instance
        List<ChangeEntry> changes = changeTrackingQuery.getUnprocessedChanges(
            lastTimestamp, lastSerial, chunkSize);
        
        if (changes.isEmpty()) {
            return 0;
        }
        
        // Get current repository state
        NodeState root = nodeStore.getRoot();
        
        // Initialize node cache for this chunk
        ChunkNodeCache cache = new ChunkNodeCache(root);
        
        // Collect impacted rules map for unified lookup of relative properties and aggregations
        Map<String, List<IndexDefinition.IndexingRule>> impactedRulesMap = collectImpactedRules(indexDefinition);
        
        // Track paths that need re-indexing (aggregation or relative properties)
        Set<String> parentReindexingPaths = new HashSet<>();
        
        // Get or create error statistics for this index
        ErrorStatistics stats = errorStatsMap.computeIfAbsent(indexPath, k -> new ErrorStatistics());
        
        // Get or create failed entries list for this index
        List<FailedEntry> failedEntries = failedEntriesMap.computeIfAbsent(indexPath, k -> new ArrayList<>());
        
        // Process each changed path
        int processed = 0;
        int successCount = 0;
        int errorCount = 0;
        boolean aborted = false;
        
        // Track progress as we iterate
        long lastProcessedTimestamp = lastTimestamp;
        long lastProcessedSerial = lastSerial;
        
        for (ChangeEntry entry : changes) {
            try {
                String path = entry.getPath();
                
                // Get the node at the changed path using cache
                NodeState node = cache.get(path);
                
                // Check if path is within index scope
                boolean isIncluded = indexDefinition.getPathFilter().filter(path) != PathFilter.Result.EXCLUDE;
                
                // Check if this path triggers parent re-indexing (relative properties or aggregations)
                Set<String> impactedParents = findImpactedParents(path, indexDefinition, impactedRulesMap, root, cache);
                
                if (!impactedParents.isEmpty()) {
                    LOG.trace("Changed path {} impacts parents: {}", path, impactedParents);
                    parentReindexingPaths.addAll(impactedParents);
                }

                if (node != null && node.exists()) {
                    // Node exists - Index the node itself if it is included in the index scope
                    if (isIncluded) {
                        LOG.trace("Indexing changed path: {}", path);
                        
                        Document doc = createLuceneDocument(path, node, indexDefinition, cache);
                        if (doc != null) {
                            indexWriter.updateDocument(path, doc);
                        } else {
                            // Node exists but has no indexed content (e.g. rule doesn't match anymore)
                            LOG.debug("Node exists but createLuceneDocument returned null for {}. Deleting.", path);
                            
                            if (indexWriter instanceof SimpleIndexWriterWrapper) {
                                // Delete ONLY this document, do not wipe descendants
                                ((SimpleIndexWriterWrapper) indexWriter).deleteDocument(path);
                            } else {
                                indexWriter.deleteDocuments(path);
                            }
                        }
                    }
                    
                    processed++;
                    successCount++;
                    stats.recordSuccess();
                    
                    // Update progress tracker
                    lastProcessedTimestamp = entry.getDiffProcessingTime();
                    lastProcessedSerial = entry.getSerialNumber();
                } else {
                    // Node was deleted - remove from index
                    
                    // Optimization: Check if path is within index scope before deleting
                    // This prevents sending delete commands for paths that this index definitely doesn't cover
                    if (isIncluded) {
                        LOG.trace("Removing deleted path from index: {}", path);
                        indexWriter.deleteDocuments(path);
                    }
                    
                    processed++;
                    successCount++;
                    stats.recordSuccess();
                    
                    // Update progress tracker
                    lastProcessedTimestamp = entry.getDiffProcessingTime();
                    lastProcessedSerial = entry.getSerialNumber();
                }
            } catch (Exception e) {
                errorCount++;
                stats.recordError();
                
                // Track this failure for potential retry
                FailedEntry failedEntry = new FailedEntry(entry, e.getMessage());
                failedEntries.add(failedEntry);
                
                LOG.error("Error processing change entry at path {}: {} (total errors: {}, error rate: {:.2f}%)",
                        entry.getPath(), e.getMessage(), stats.totalErrors, stats.getErrorRate());
                
                // Check circuit breaker
                if (stats.shouldTriggerCircuitBreaker()) {
                    LOG.error("CIRCUIT BREAKER TRIGGERED for index {}: Error rate {:.2f}% exceeds threshold {}%. " +
                              "Stopping processing to prevent system degradation. " +
                              "Processed: {}, Errors: {}, Consecutive Errors: {}",
                              indexPath, stats.getErrorRate(), ERROR_RATE_CIRCUIT_BREAKER_THRESHOLD,
                              stats.totalProcessed, stats.totalErrors, stats.consecutiveErrors);
                    
                    // Break out of processing loop
                    aborted = true;
                    break;
                }
                
                // Continue with next entry - don't fail entire chunk for one error
            }
        }
        
        // Log summary with error statistics
        LOG.info("Chunk processing for index {}: Processed={}, Success={}, Errors={}, Error Rate={:.2f}%, " +
                 "Total Errors={}, Consecutive Errors={}",
                 indexPath, processed, successCount, errorCount, stats.getErrorRate(),
                 stats.totalErrors, stats.consecutiveErrors);
        
        // Warn if error rate is elevated
        if (stats.shouldWarn() && !stats.shouldTriggerCircuitBreaker()) {
            LOG.warn("ELEVATED ERROR RATE for index {}: {:.2f}% (threshold: {}%). " +
                     "System health may be degraded. Failed entries: {}",
                     indexPath, stats.getErrorRate(), ERROR_RATE_WARNING_THRESHOLD, failedEntries.size());
        }
        
        // Process batched parent re-indexing (relative properties and aggregations)
        if (!parentReindexingPaths.isEmpty() && !aborted) {
            LOG.debug("Re-indexing {} parent nodes for relative properties/aggregations", parentReindexingPaths.size());
            for (String parentPath : parentReindexingPaths) {
                try {
                    // Use cache for re-indexing lookups too
                    NodeState parentNode = cache.get(parentPath);
                    if (parentNode != null && parentNode.exists()) {
                        Document doc = createLuceneDocument(parentPath, parentNode, indexDefinition, cache);
                        if (doc != null) {
                            indexWriter.updateDocument(parentPath, doc);
                            LOG.trace("Re-indexed parent node {}", parentPath);
                        } else {
                            // Parent exists but has no indexed content (e.g. aggregation removed)
                            if (indexWriter instanceof SimpleIndexWriterWrapper) {
                                // Delete ONLY this document, do not wipe descendants
                                ((SimpleIndexWriterWrapper) indexWriter).deleteDocument(parentPath);
                            } else {
                                indexWriter.deleteDocuments(parentPath);
                            }
                            LOG.trace("Removed parent node {} from index (no content)", parentPath);
                        }
                    } else {
                        // Parent no longer exists, remove it
                        indexWriter.deleteDocuments(parentPath);
                        LOG.trace("Removed non-existent parent node {} from index", parentPath);
                    }
                } catch (Exception e) {
                    LOG.warn("Error re-indexing parent path {}: {}", parentPath, e.getMessage());
                }
            }
        }
        
        // Update progress metadata
        if (processed > 0) {
            metadataManager.updateProgress(
                indexPath,
                lastProcessedTimestamp,
                lastProcessedSerial,
                processed
            );
            
            LOG.info("Updated progress for {}: processed {} changes", indexPath, processed);
        }
        
        return processed;
    }
    
    /**
     * Processes all pending changes for an index in chunks until complete.
     * 
     * @param indexPath the path of the index definition
     * @param indexDefinition the index definition
     * @param indexWriter the Lucene index writer
     * @return the total number of changes processed
     * @throws IOException if reading or writing fails
     * @throws CommitFailedException if committing changes fails
     */
    public int processAllChanges(@NotNull String indexPath,
                                   @NotNull IndexDefinition indexDefinition,
                                   @NotNull LuceneIndexWriter indexWriter)
            throws IOException, CommitFailedException {
        
        LOG.info("Processing all pending changes for index: {}", indexPath);
        
        int totalProcessed = 0;
        int chunkCount = 0;
        
        while (true) {
            int processedInChunk = processChangesChunk(indexPath, indexDefinition, indexWriter);
            
            if (processedInChunk == 0) {
                // No more changes to process
                break;
            }
            
            totalProcessed += processedInChunk;
            chunkCount++;
            
            LOG.info("Completed chunk {} for index {}: {} changes ({} total)", 
                    chunkCount, indexPath, processedInChunk, totalProcessed);
            
            // Commit after each chunk to avoid large transactions
            indexWriter.close((int) System.currentTimeMillis());
        }
        
        LOG.info("Completed processing for index {}: {} changes in {} chunks", 
                indexPath, totalProcessed, chunkCount);
        
        return totalProcessed;
    }
    
    /**
     * Creates a Lucene document for a node using Oak's index rules and property definitions.
     * 
     * @param path the path of the node
     * @param node the node state
     * @param indexDefinition the index definition with rules and property definitions
     * @param cache the chunk node cache
     * @return a Lucene document, or null if the node shouldn't be indexed
     */
    private Document createLuceneDocument(String path, NodeState node, IndexDefinition indexDefinition, ChunkNodeCache cache) {
        // Step 1: Get node's primary type
        org.apache.jackrabbit.oak.api.PropertyState primaryTypeProp = node.getProperty("jcr:primaryType");
        if (primaryTypeProp == null) {
            // System.out.println("DEBUG: Skipping " + path + " - no primary type");
            return null;
        }
        String nodeType = primaryTypeProp.getValue(org.apache.jackrabbit.oak.api.Type.STRING);
        
        // Step 2: Get applicable indexing rule for this node type
        IndexDefinition.IndexingRule rule = indexDefinition.getApplicableIndexingRule(nodeType);
        if (rule == null) {
            // LOG.trace("Skipping {} - no rule for type {}", path, nodeType);
            return null;
        }
        
        // Step 3: Check if node should be indexed (rule conditions)
        if (!rule.appliesTo(node)) {
             // System.out.println("DEBUG: Skipping " + path + " - rule does not apply to node state");
            return null;
        }
        
        // Step 4: Create Lucene document
        Document doc = new Document();
        // System.out.println("DEBUG: Creating document for " + path + " (type: " + nodeType + ")"); // SYSOUT
        
        // Always add path field using Oak's factory method
        doc.add(FieldFactory.newPathField(path));
        
        // Step 5: Index properties according to property definitions
        boolean hasIndexedContent = false;
        for (PropertyDefinition propDef : rule.getProperties()) {
            try {
                boolean indexed = indexProperty(doc, node, propDef, path, cache);
                if (indexed) {
                    hasIndexedContent = true;
                    // System.out.println("DEBUG: Indexed property: " + propDef.name + " for " + path); // SYSOUT
                }
            } catch (Exception e) {
                LOG.warn("Error indexing property {} at path {}: {}", 
                        propDef.name, path, e.getMessage());
                // Continue with other properties
            }
        }
        
        // Step 6: Handle node-scoped fulltext (if enabled)
        if (rule.isFulltextEnabled()) {
            // Node-scoped fulltext combines all analyzed properties
            // This enables queries like: SELECT * FROM [nt:base] WHERE CONTAINS(*, 'search')
            // Implementation: Collect all text from analyzed properties into a special field
            String nodeScopedText = collectNodeScopedText(node, rule, path, cache);
            if (nodeScopedText != null && !nodeScopedText.isEmpty()) {
                // Use Oak's factory method for consistency
                doc.add(FieldFactory.newFulltextField(nodeScopedText));
                hasIndexedContent = true;
                LOG.trace("Added node-scoped fulltext for {}", path);
            } else {
                LOG.trace("No node-scoped fulltext content for {}", path);
            }
        }
        
        // Only return document if it has indexed content beyond just the path
        if (hasIndexedContent) {
             return doc;
        } else {
             LOG.trace("No indexed content for {}, returning null doc", path);
             return null;
        }
    }
    
    /**
     * Indexes a single property according to its PropertyDefinition.
     * Handles relative properties, property types, analyzed vs not-analyzed, stored vs not-stored.
     * 
     * @param doc the Lucene document being built
     * @param node the node being indexed
     * @param propDef the property definition from index rules
     * @param nodePath the node path (for logging/debugging)
     * @param cache the chunk node cache
     * @return true if the property was indexed, false otherwise
     */
    private boolean indexProperty(Document doc, NodeState node, PropertyDefinition propDef,
                                  String nodePath, ChunkNodeCache cache) {
        org.apache.jackrabbit.oak.api.PropertyState prop;
        
        // Handle relative properties (e.g., jcr:content/jcr:data for nt:file)
        if (propDef.relative && propDef.ancestors != null && propDef.ancestors.length > 0) {
            String relativePath = String.join("/", propDef.ancestors);
            NodeState relativeNode = getNodeAtRelativePath(node, relativePath, cache);
            if (relativeNode == null || !relativeNode.exists()) {
                return false;
            }
            prop = relativeNode.getProperty(propDef.nonRelativeName != null ? propDef.nonRelativeName : propDef.name);
        } else {
            // Direct property
            prop = node.getProperty(propDef.name);
        }
        
        if (prop == null) {
            return false;
        }
        
        // Add property to document using configuration from PropertyDefinition
        return addPropertyToDocument(doc, prop, propDef);
    }
    
    /**
     * Adds a property to the Lucene document respecting PropertyDefinition configuration.
     * 
     * @param doc the Lucene document
     * @param prop the property state
     * @param propDef the property definition with configuration
     * @return true if property was added
     */
    private boolean addPropertyToDocument(Document doc, org.apache.jackrabbit.oak.api.PropertyState prop,
                                          PropertyDefinition propDef) {
        Field.Store store = propDef.stored ? Field.Store.YES : Field.Store.NO;
        boolean added = false;
        
        // Handle function indexes (LIMITATION 1.4 - to be implemented)
        if (propDef.function != null) {
            // TODO: Apply function transformation
            LOG.trace("Function indexes not yet supported: {}", propDef.function);
            // For now, fallthrough to index the raw value
        }
        
        // Determine field type based on property definition
        if (prop.isArray()) {
            // Handle multi-value properties
            for (int i = 0; i < prop.count(); i++) {
                added |= addFieldsForValue(doc, prop, propDef, store, i);
            }
        } else {
            // Handle single-value property
            added |= addFieldsForValue(doc, prop, propDef, store, -1);
        }
        
        return added;
    }

    /**
     * Adds fields for a single property value (analyzed and/or typed).
     */
    private boolean addFieldsForValue(Document doc, org.apache.jackrabbit.oak.api.PropertyState prop, 
                                      PropertyDefinition propDef, Field.Store store, int index) {
        boolean added = false;
        
        // 1. Analyzed field (for fulltext search)
        if (propDef.analyzed) {
            Field field = createAnalyzedField(prop, propDef, store, index);
            if (field != null) {
                // Apply boosting if configured
                if (propDef.boost != 1.0f) {
                    field.setBoost(propDef.boost);
                }
                doc.add(field);
                added = true;
            }
        }
        
        // 2. Typed field (for equality/range queries)
        // Add if analyzed is false (must have typed field) OR if explicitly requested via propertyIndex
        if (!propDef.analyzed || propDef.propertyIndex) {
            Field field = createTypedField(prop, propDef, store, index);
            if (field != null) {
                // Apply boosting if configured
                if (propDef.boost != 1.0f) {
                    field.setBoost(propDef.boost);
                }
                doc.add(field);
                added = true;
            }
        }
        
        return added;
    }
    
    /**
     * Creates an analyzed text field.
     */
    private Field createAnalyzedField(org.apache.jackrabbit.oak.api.PropertyState prop,
                                      PropertyDefinition propDef,
                                      Field.Store store,
                                      int index) {
        try {
            String value = index >= 0 ?
                prop.getValue(org.apache.jackrabbit.oak.api.Type.STRING, index) :
                prop.getValue(org.apache.jackrabbit.oak.api.Type.STRING);
                
            if (value != null && !value.isEmpty()) {
                // Use Oak's field naming convention for analyzed properties
                String analyzedFieldName = FieldNames.createAnalyzedFieldName(propDef.name);
                
                // Use Oak's field factory to create proper field
                Field field = FieldFactory.newPropertyField(
                    analyzedFieldName,           // "full:propertyName"
                    value,
                    true,                        // tokenized
                    propDef.stored
                );
                return field;
            }
        } catch (Exception e) {
            LOG.warn("Error creating analyzed field for property {}: {}", propDef.name, e.getMessage());
        }
        return null;
    }

    /**
     * Creates a typed field (String, Long, Double, Date) for exact match or range queries.
     */
    private Field createTypedField(org.apache.jackrabbit.oak.api.PropertyState prop,
                                   PropertyDefinition propDef,
                                   Field.Store store,
                                   int index) {
        int propType = prop.getType().tag();
        
        try {
            switch (propType) {
                case PropertyType.LONG:
                    Long longValue = index >= 0 ?
                        prop.getValue(org.apache.jackrabbit.oak.api.Type.LONG, index) :
                        prop.getValue(org.apache.jackrabbit.oak.api.Type.LONG);
                    if (longValue != null) {
                        return new LongField(propDef.name, longValue, store);
                    }
                    break;
                    
                case PropertyType.DOUBLE:
                    Double doubleValue = index >= 0 ?
                        prop.getValue(org.apache.jackrabbit.oak.api.Type.DOUBLE, index) :
                        prop.getValue(org.apache.jackrabbit.oak.api.Type.DOUBLE);
                    if (doubleValue != null) {
                        return new DoubleField(propDef.name, doubleValue, store);
                    }
                    break;
                    
                case PropertyType.DATE:
                    // Convert date to long (milliseconds) for range queries
                    String dateStr = index >= 0 ?
                        prop.getValue(org.apache.jackrabbit.oak.api.Type.DATE, index) :
                        prop.getValue(org.apache.jackrabbit.oak.api.Type.DATE);
                    if (dateStr != null) {
                        // Parse ISO 8601 date string to milliseconds
                        long dateMillis = javax.xml.bind.DatatypeConverter.parseDateTime(dateStr)
                            .getTimeInMillis();
                        return new LongField(propDef.name, dateMillis, store);
                    }
                    break;
                    
                case PropertyType.BINARY:
                    // Binary properties: extract text if configured
                    LOG.trace("Binary property {} skipped (text extraction not configured)", propDef.name);
                    return null;
                    
                default:
                    // String and other types: use StringField for exact match
                    String value = index >= 0 ?
                        prop.getValue(org.apache.jackrabbit.oak.api.Type.STRING, index) :
                        prop.getValue(org.apache.jackrabbit.oak.api.Type.STRING);
                    if (value != null && !value.isEmpty()) {
                        // LOG.trace("Created typed field {}={} for {}", propDef.name, value, propDef.name);
                        return new StringField(propDef.name, value, store);
                    }
            }
        } catch (Exception e) {
            LOG.warn("Error creating typed field for property {}: {}", propDef.name, e.getMessage());
        }
        
        return null;
    }
    
    /**
     * Gets a node at a relative path from a base node.
     * Used for indexing relative properties like jcr:content/jcr:data.
     * 
     * @param base the base node
     * @param relativePath the relative path (e.g., "jcr:content" or "jcr:content/metadata")
     * @param cache the chunk node cache
     * @return the node at the relative path, or null if it doesn't exist
     */
    private NodeState getNodeAtRelativePath(NodeState base, String relativePath, ChunkNodeCache cache) {
        if (relativePath == null || relativePath.isEmpty()) {
            return base;
        }
        
        NodeState current = base;
        String[] segments = relativePath.split("/");
        
        for (String segment : segments) {
            if (segment.isEmpty()) continue;
            current = current.getChildNode(segment);
            if (!current.exists()) {
                return null;
            }
        }
        
        return current;
    }
    
    /**
     * Collects node-scoped fulltext content from all analyzed properties and aggregations.
     * This enables queries like: SELECT * FROM [nt:base] WHERE CONTAINS(*, 'search')
     * 
     * @param node the node being indexed
     * @param rule the indexing rule
     * @param path the node path
     * @param cache the chunk node cache
     * @return combined text from all analyzed properties and aggregations, or null if none
     */
    private String collectNodeScopedText(NodeState node, IndexDefinition.IndexingRule rule, String path, ChunkNodeCache cache) {
        StringBuilder fulltext = new StringBuilder();
        
        // 1. Properties of the current node
        for (PropertyDefinition propDef : rule.getProperties()) {
            if (!propDef.analyzed || propDef.nodeScopeIndex == false) {
                continue;  // Only include analyzed properties in node-scoped index
            }
            
            org.apache.jackrabbit.oak.api.PropertyState prop;
            if (propDef.relative && propDef.ancestors != null && propDef.ancestors.length > 0) {
                String relativePath = String.join("/", propDef.ancestors);
                NodeState relativeNode = getNodeAtRelativePath(node, relativePath, cache);
                if (relativeNode == null || !relativeNode.exists()) {
                    continue;
                }
                prop = relativeNode.getProperty(propDef.nonRelativeName != null ? propDef.nonRelativeName : propDef.name);
            } else {
                prop = node.getProperty(propDef.name);
            }
            
            if (prop != null) {
                collectTextFromProperty(prop, fulltext);
            }
        }
        
        // 2. Aggregations (content from child nodes)
        collectAggregatedText(node, rule, path, fulltext);
        
        return fulltext.length() > 0 ? fulltext.toString() : null;
    }
    
    /**
     * Collects text from aggregations.
     */
    private void collectAggregatedText(NodeState node, IndexDefinition.IndexingRule rule, String path, StringBuilder fulltext) {
        Aggregate aggregate = rule.getAggregate();
        if (aggregate == null) {
            return;
        }
        
        try {
            // Use Oak's Aggregate API to collect content from aggregated nodes
            aggregate.collectAggregates(node, new Aggregate.ResultCollector() {
                @Override
                public void onResult(Aggregate.NodeIncludeResult result) {
                    // Collect text from all properties of the aggregated node
                    if (result.nodeState != null) {
                        for (org.apache.jackrabbit.oak.api.PropertyState prop : result.nodeState.getProperties()) {
                            if (prop.getType().tag() == PropertyType.STRING) {
                                collectTextFromProperty(prop, fulltext);
                            }
                        }
                    }
                }

                @Override
                public void onResult(Aggregate.PropertyIncludeResult result) {
                    // Collect text from specific included property
                    if (result.propertyState != null) {
                        collectTextFromProperty(result.propertyState, fulltext);
                    }
                }
            });
        } catch (Exception e) {
            LOG.warn("Error collecting aggregated text for path {}: {}", path, e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Helper to collect text from a property.
     */
    private void collectTextFromProperty(org.apache.jackrabbit.oak.api.PropertyState prop, StringBuilder fulltext) {
        try {
            if (prop.isArray()) {
                for (int i = 0; i < prop.count(); i++) {
                    String value = prop.getValue(org.apache.jackrabbit.oak.api.Type.STRING, i);
                    if (value != null && !value.isEmpty()) {
                        if (fulltext.length() > 0) fulltext.append(" ");
                        fulltext.append(value);
                    }
                }
            } else {
                String value = prop.getValue(org.apache.jackrabbit.oak.api.Type.STRING);
                if (value != null && !value.isEmpty()) {
                    if (fulltext.length() > 0) fulltext.append(" ");
                    fulltext.append(value);
                }
            }
        } catch (Exception e) {
            // Ignore conversion errors
        }
    }
    
    /**
     * Calculates the maximum relative depth defined in the index rules (aggregation + relative properties).
     */
    private int getRelativeDepth(IndexDefinition indexDefinition) {
        int maxDepth = 0;
        for (IndexDefinition.IndexingRule rule : indexDefinition.getDefinedRules()) {
            Aggregate aggregate = rule.getAggregate();
            if (aggregate != null) {
                // Check recursion limit configured for this aggregate
                if (aggregate.reAggregationLimit > maxDepth) {
                    maxDepth = aggregate.reAggregationLimit;
                }
                
                // Also check depth of includes
                for (Aggregate.Include include : aggregate.getIncludes()) {
                    if (include instanceof Aggregate.NodeInclude) {
                        // include.maxDepth() returns the number of path elements
                        int depth = include.maxDepth();
                        if (depth > maxDepth) {
                            maxDepth = depth;
                        }
                    }
                }
            }
            
            // Also check relative properties as they imply aggregation/dependency
            for (PropertyDefinition pd : rule.getProperties()) {
                if (pd.relative && pd.ancestors != null) {
                    int depth = pd.ancestors.length;
                    if (depth > maxDepth) {
                        maxDepth = depth;
                    }
                }
            }
        }
        // Ensure at least a minimum reasonable depth if aggregation is enabled
        return maxDepth > 0 ? maxDepth : 0;
    }

    /**
     * Collects all impacted paths (aggregation includes and relative properties) from the index rules.
     * 
     * @return Map where key is the relative path pattern and value is the list of indexing rules that use this path.
     */
    private Map<String, List<IndexDefinition.IndexingRule>> collectImpactedRules(IndexDefinition indexDefinition) {
        Map<String, List<IndexDefinition.IndexingRule>> impactedRules = new HashMap<>();
        
        for (IndexDefinition.IndexingRule rule : indexDefinition.getDefinedRules()) {
            // 1. Aggregations
            Aggregate aggregate = rule.getAggregate();
            if (aggregate != null) {
                for (Aggregate.Include include : aggregate.getIncludes()) {
                    if (include instanceof Aggregate.NodeInclude) {
                        String pattern = ((Aggregate.NodeInclude) include).getPattern();
                        if (pattern != null && !pattern.isEmpty()) {
                            impactedRules.computeIfAbsent(pattern, k -> new ArrayList<>()).add(rule);
                        }
                    }
                }
            }
            
            // 2. Relative properties
            for (PropertyDefinition pd : rule.getProperties()) {
                if (pd.relative && pd.ancestors != null && pd.ancestors.length > 0) {
                    String relativePath = String.join("/", pd.ancestors);
                    if (pd.nonRelativeName != null && !pd.nonRelativeName.isEmpty()) {
                        relativePath += "/" + pd.nonRelativeName;
                    } else {
                        relativePath += "/" + pd.name;
                    }
                    impactedRules.computeIfAbsent(relativePath, k -> new ArrayList<>()).add(rule);
                }
            }
        }
        return impactedRules;
    }

    /**
     * Finds parent nodes that need to be indexed when a child path changes.
     * This unifies logic for both aggregations and relative properties.
     * 
     * @param changedPath the path that changed
     * @param indexDefinition the index definition (for max depth calculation)
     * @param impactedRulesMap pre-calculated map of relative paths to rules
     * @param root root node state for lookups
     * @param cache the chunk node cache
     * @return set of parent paths that need to be indexed
     */
    private Set<String> findImpactedParents(
            String changedPath,
            IndexDefinition indexDefinition,
            Map<String, List<IndexDefinition.IndexingRule>> impactedRulesMap,
            NodeState root,
            ChunkNodeCache cache) {
        
        Set<String> parentPaths = new HashSet<>();
        String currentPath = changedPath;
        
        // Dynamically determine max traversal depth based on index rules
        int maxLevels = getRelativeDepth(indexDefinition) + 2;
        int level = 0;
        
        while (level < maxLevels) {
            String parentPath = getParentPath(currentPath);
            if (parentPath == null) break;
            
            String relativePath = changedPath.substring(parentPath.length() + 1);
            
            // Check all patterns to see if this relative path matches any dependency
            for (Map.Entry<String, List<IndexDefinition.IndexingRule>> entry : impactedRulesMap.entrySet()) {
                String pattern = entry.getKey();
                boolean match = false;
                
                // 1. Exact match (e.g. property changed)
                if (relativePath.equals(pattern)) {
                    match = true;
                } 
                // 2. Descendant match (change inside aggregated node)
                // e.g. pattern="jcr:content", relativePath="jcr:content/metadata"
                else if (relativePath.startsWith(pattern + "/")) {
                    match = true;
                }
                // 3. Ancestor match (intermediate node changed/deleted)
                // e.g. pattern="jcr:content/metadata/title", relativePath="jcr:content/metadata"
                else if (pattern.startsWith(relativePath + "/")) {
                    match = true;
                }
                
                if (match) {
                    // Pattern matches, check if parent node exists and matches rule
                    NodeState parentNode = cache.get(parentPath);
                    if (parentNode != null && parentNode.exists()) {
                        for (IndexDefinition.IndexingRule rule : entry.getValue()) {
                            if (rule.appliesTo(parentNode)) {
                                parentPaths.add(parentPath);
                                LOG.trace("Found parent path {} needs re-indexing (rule: {}, pattern: {}, relativePath: {})",
                                        parentPath, rule.getNodeTypeName(), pattern, relativePath);
                                break; // Found valid rule for this parent
                            }
                        }
                    }
                    
                    if (parentPaths.contains(parentPath)) {
                        break; // Optimization: Parent already added, move to next ancestor level
                    }
                }
            }
            
            currentPath = parentPath;
            level++;
        }
        return parentPaths;
    }
    
    /**
     * Gets the parent path of a given path.
     */
    private String getParentPath(String path) {
        if (path == null || path.equals("/")) {
            return null;
        }
        int lastSlash = path.lastIndexOf('/');
        if (lastSlash == 0) {
            return "/";
        }
        if (lastSlash == -1) {
            return null;
        }
        return path.substring(0, lastSlash);
    }
    
    /**
     * Gets the configured chunk size.
     */
    public int getChunkSize() {
        return chunkSize;
    }
    
    /**
     * Gets failed entries for an index (for retry or monitoring).
     * 
     * @param indexPath the index path
     * @return list of failed entries, or empty list if none
     */
    public List<FailedEntry> getFailedEntries(String indexPath) {
        return failedEntriesMap.getOrDefault(indexPath, new ArrayList<>());
    }
    
    /**
     * Gets error statistics for an index.
     * 
     * @param indexPath the index path
     * @return error statistics, or null if no processing has occurred
     */
    public ErrorStatistics getErrorStatistics(String indexPath) {
        return errorStatsMap.get(indexPath);
    }
    
    /**
     * Clears failed entries for an index (e.g., after successful retry or manual intervention).
     * 
     * @param indexPath the index path
     * @return the number of entries cleared
     */
    public int clearFailedEntries(String indexPath) {
        List<FailedEntry> entries = failedEntriesMap.remove(indexPath);
        return entries != null ? entries.size() : 0;
    }
    
    /**
     * Resets error statistics for an index (e.g., after fixing underlying issues).
     * 
     * @param indexPath the index path
     */
    public void resetErrorStatistics(String indexPath) {
        errorStatsMap.remove(indexPath);
        LOG.info("Reset error statistics for index {}", indexPath);
    }
    
    /**
     * Retries failed entries for an index.
     * 
     * <p>This method attempts to reprocess entries that previously failed.
     * Entries that succeed are removed from the failed list.
     * Entries that fail again have their retry count incremented.
     * 
     * @param indexPath the index path
     * @param indexDefinition the index definition
     * @param indexWriter the Lucene index writer
     * @return the number of entries successfully retried
     * @throws IOException if reading or writing fails
     */
    public int retryFailedEntries(String indexPath, 
                                  IndexDefinition indexDefinition,
                                  LuceneIndexWriter indexWriter) throws IOException {
        List<FailedEntry> failures = failedEntriesMap.get(indexPath);
        if (failures == null || failures.isEmpty()) {
            LOG.debug("No failed entries to retry for index {}", indexPath);
            return 0;
        }
        
        LOG.info("Retrying {} failed entries for index {}", failures.size(), indexPath);
        
        NodeState root = nodeStore.getRoot();
        ChunkNodeCache cache = new ChunkNodeCache(root);
        List<FailedEntry> stillFailing = new ArrayList<>();
        int successCount = 0;
        
        for (FailedEntry failedEntry : failures) {
            ChangeEntry entry = failedEntry.entry;
            failedEntry.retryCount++;
            
            try {
                String path = entry.getPath();
                NodeState node = cache.get(path);
                
                if (node != null && node.exists()) {
                    Document doc = createLuceneDocument(path, node, indexDefinition, cache);
                    if (doc != null) {
                        indexWriter.updateDocument(path, doc);
                    }
                } else {
                    indexWriter.deleteDocuments(path);
                }
                
                successCount++;
                LOG.debug("Successfully retried entry at path {} (retry #{})", 
                         path, failedEntry.retryCount);
                
            } catch (Exception e) {
                LOG.warn("Failed to retry entry at path {} (retry #{}): {}",
                        failedEntry.entry.getPath(), failedEntry.retryCount, e.getMessage());
                stillFailing.add(failedEntry);
            }
        }
        
        // Update failed entries list
        if (stillFailing.isEmpty()) {
            failedEntriesMap.remove(indexPath);
            LOG.info("All {} failed entries successfully retried for index {}", 
                    failures.size(), indexPath);
        } else {
            failedEntriesMap.put(indexPath, stillFailing);
            LOG.info("Retry completed for index {}: {} succeeded, {} still failing",
                    indexPath, successCount, stillFailing.size());
        }
        
        return successCount;
    }

    
    private static class ChunkNodeCache {
        private static final String PROP_CACHE_SIZE = "oak.changeTracker.nodeCacheSize";
        // Default 16MB
        private static final int DEFAULT_CACHE_SIZE = 16 * 1024 * 1024;
        
        private final NodeState root;
        // Limit cache size (bytes) to avoid OOM during large chunk processing
        private final int maxCacheWeight;
        private final Map<String, NodeState> cache = new HashMap<>();
        private long currentWeight = 0;
        
        ChunkNodeCache(NodeState root) {
            this.root = root;
            this.maxCacheWeight = Integer.getInteger(PROP_CACHE_SIZE, DEFAULT_CACHE_SIZE);
            cache.put("/", root);
            // Initial weight: minimal
            this.currentWeight = 1024; 
        }
        
        NodeState get(String path) {
            if (path == null) return null;
            if (path.equals("/")) return root;
            
            NodeState cached = cache.get(path);
            if (cached != null) return cached;
            
            // Check eviction before adding new entry
            // Estimate weight: path length * 2 (chars) + 1024 bytes overhead per entry
            // 1KB is a safe upper bound estimate for NodeState reference + Map entry overhead
            int estimatedWeight = (path.length() * 2) + 1024;
            
            // Prevent cache from growing unbounded
            if (currentWeight + estimatedWeight >= maxCacheWeight) {
                // Simple eviction strategy: clear all if limit reached
                // This is acceptable as cache is scoped to a single chunk
                cache.clear();
                cache.put("/", root);
                currentWeight = 1024;
            }
            
            // Not in cache, find closest ancestor
            String parentPath = getParentPath(path);
            if (parentPath == null) return null;
            
            NodeState parent = get(parentPath); // Recursive call to populate ancestors
            if (parent == null || !parent.exists()) {
                // Parent doesn't exist, so child doesn't exist
                return null;
            }
            
            String name = getName(path);
            NodeState node = parent.getChildNode(name);
            // Cache even if it doesn't exist (it will be non-existent NodeState)
            cache.put(path, node);
            currentWeight += estimatedWeight;
            
            return node;
        }
        
        private String getParentPath(String path) {
            int lastSlash = path.lastIndexOf('/');
            if (lastSlash == 0) return "/";
            if (lastSlash == -1) return null;
            return path.substring(0, lastSlash);
        }
        
        private String getName(String path) {
            int lastSlash = path.lastIndexOf('/');
            if (lastSlash == -1) return path;
            return path.substring(lastSlash + 1);
        }
    }
}
