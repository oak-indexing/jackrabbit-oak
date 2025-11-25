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
package org.apache.jackrabbit.oak.plugins.index.lucene.changetracker.perf;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.util.ISO8601;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;

/**
 * Utility for creating and manipulating DAM assets for performance testing.
 * 
 * <p>Creates realistic AEM DAM asset structure matching damAssetLucene-13's 12 aggregates:
 * <pre>
 * /content/dam/assets/
 *   + asset-0001 (dam:Asset)
 *     + jcr:content (dam:AssetContent)                    [include0]
 *       + metadata (nt:unstructured)                      [include1]
 *         - dc:title = "Asset 0001"                       [include2]
 *         - dc:format = "image/jpeg"                      [include2]
 *         - dam:status = "approved"                       [include2]
 *         - cq:tags = ["product:camera"]                  [include2]
 *         - dam:size = 2048576                            [include2]
 *         - jcr:lastModified = 2025-11-25T10:00:00        [include2]
 *         - dam:sha1 = "abc123..."                        [include2]
 *       + renditions (nt:folder)                          [include3]
 *         + original (nt:file)                            [include4]
 *           + jcr:content (nt:resource)                   [include5]
 *             - jcr:mimeType = "image/jpeg"
 *         + thumbnail (nt:file)
 *           + jcr:content (nt:resource)
 *         + cqdam.text.txt (nt:file)
 *           + jcr:content (nt:resource)                   [include10]
 *             - jcr:data = "extracted text..."
 *       + comments (nt:unstructured)                      [include6]
 *         + comment1 (nt:unstructured)                    [include7]
 *           - text = "Approved for use"
 *       + data (nt:unstructured)
 *         + master (nt:unstructured)                      [include8]
 *           - contentFragment = true
 *       + usages (nt:unstructured)                        [include9]
 *         - usedBy = ["/content/site/page1"]
 * </pre>
 * 
 * <p>Total nodes per asset: ~15 nodes (covers all 12 aggregates)
 */
public class DamAssetCreator {
    
    private static final Logger LOG = LoggerFactory.getLogger(DamAssetCreator.class);
    
    private static final String[] FORMATS = {
        "image/jpeg", "image/png", "image/gif", "image/svg+xml",
        "video/mp4", "video/quicktime",
        "application/pdf", "application/zip"
    };
    
    private static final String[] STATUSES = {
        "approved", "pending", "rejected", "draft"
    };
    
    private static final String[][] TAG_CATEGORIES = {
        {"product:camera", "product:lens", "product:tripod", "product:lighting"},
        {"season:summer", "season:winter", "season:spring", "season:fall"},
        {"event:wedding", "event:corporate", "event:sports", "event:concert"},
        {"location:indoor", "location:outdoor", "location:studio"},
        {"style:modern", "style:vintage", "style:minimal", "style:dramatic"}
    };
    
    private static final Random RANDOM = new Random(42); // Fixed seed for reproducibility
    
    /**
     * Creates a batch of DAM assets.
     * 
     * @param root Root to create assets in
     * @param count Number of assets to create
     * @param startIndex Starting index for asset numbering
     */
    public static void createAssets(Root root, int count, int startIndex) {
        Tree contentDam = ensurePath(root, "/content/dam/assets");
        
        long startTime = System.currentTimeMillis();
        int logInterval = Math.max(1, count / 10); // Log 10 times during creation
        
        for (int i = 0; i < count; i++) {
            int assetNum = startIndex + i;
            String assetName = String.format("asset-%06d", assetNum);
            
            createAsset(contentDam, assetName, assetNum);
            
            if ((i + 1) % logInterval == 0 || (i + 1) == count) {
                long elapsed = System.currentTimeMillis() - startTime;
                double rate = (i + 1) * 1000.0 / elapsed;
                LOG.debug("  Created {}/{} assets ({} assets/sec)", i + 1, count, String.format("%.1f", rate));
            }
        }
    }
    
    /**
     * Updates metadata for existing assets.
     * 
     * @param root Root containing assets
     * @param count Number of assets to update
     */
    public static void updateAssetMetadata(Root root, int count) {
        Tree contentDam = root.getTree("/content/dam/assets");
        if (!contentDam.exists()) {
            LOG.warn("No assets found to update");
            return;
        }
        
        int updated = 0;
        for (Tree asset : contentDam.getChildren()) {
            if (updated >= count) break;
            
            Tree metadata = asset.getChild("jcr:content").getChild("metadata");
            if (metadata.exists()) {
                // Update status
                String newStatus = STATUSES[RANDOM.nextInt(STATUSES.length)];
                metadata.setProperty("dam:status", newStatus);
                
                // Update timestamp
                Calendar updateCal = Calendar.getInstance();
                metadata.setProperty("jcr:lastModified", ISO8601.format(updateCal));
                
                // Update tags (add one more tag)
                String[] currentTags = getPropertyArray(metadata, "cq:tags");
                String newTag = getRandomTag();
                String[] newTags = Arrays.copyOf(currentTags, currentTags.length + 1);
                newTags[currentTags.length] = newTag;
                metadata.setProperty("cq:tags", Arrays.asList(newTags), Type.STRINGS);
                
                updated++;
            }
        }
        
        LOG.debug("  Updated metadata for {} assets", updated);
    }
    
    /**
     * Updates renditions (child nodes) for existing assets.
     * This tests aggregation re-indexing.
     * 
     * @param root Root containing assets
     * @param count Number of assets to update
     */
    public static void updateAssetRenditions(Root root, int count) {
        Tree contentDam = root.getTree("/content/dam/assets");
        if (!contentDam.exists()) {
            LOG.warn("No assets found to update");
            return;
        }
        
        int updated = 0;
        for (Tree asset : contentDam.getChildren()) {
            if (updated >= count) break;
            
            Tree renditions = asset.getChild("jcr:content").getChild("renditions");
            if (renditions.exists()) {
                // Add a new rendition
                String renditionName = "web-" + System.currentTimeMillis();
                Tree newRendition = renditions.addChild(renditionName);
                newRendition.setProperty("jcr:primaryType", "nt:file", Type.NAME);
                
                Tree jcrContent = newRendition.addChild("jcr:content");
                jcrContent.setProperty("jcr:primaryType", "nt:resource", Type.NAME);
                jcrContent.setProperty("jcr:mimeType", "image/jpeg");
                Calendar updateCal = Calendar.getInstance();
                jcrContent.setProperty("jcr:lastModified", ISO8601.format(updateCal));
                
                updated++;
            }
        }
        
        LOG.debug("  Updated renditions for {} assets", updated);
    }
    
    // ========================================
    // Private Helper Methods
    // ========================================
    
    private static void createAsset(Tree parent, String assetName, int assetNum) {
        // Create asset node (dam:Asset)
        Tree asset = parent.addChild(assetName);
        asset.setProperty("jcr:primaryType", "dam:Asset", Type.NAME);
        
        // Create jcr:content (dam:AssetContent) [include0]
        Tree jcrContent = asset.addChild("jcr:content");
        jcrContent.setProperty("jcr:primaryType", "dam:AssetContent", Type.NAME);
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(System.currentTimeMillis() - (assetNum * 1000L));
        jcrContent.setProperty("jcr:lastModified", ISO8601.format(cal));
        
        // Create metadata [include1]
        Tree metadata = jcrContent.addChild("metadata");
        metadata.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        
        // Set metadata properties [include2 - metadata/*]
        metadata.setProperty("dc:title", "Asset " + String.format("%06d", assetNum));
        metadata.setProperty("dc:format", FORMATS[assetNum % FORMATS.length]);
        metadata.setProperty("dam:status", STATUSES[assetNum % STATUSES.length]);
        
        // Multi-value tags (2-3 tags per asset)
        int tagCount = 2 + (assetNum % 2);
        String[] tags = new String[tagCount];
        for (int i = 0; i < tagCount; i++) {
            tags[i] = getRandomTag();
        }
        metadata.setProperty("cq:tags", Arrays.asList(tags), Type.STRINGS);
        
        // Size (1MB - 10MB range)
        long size = 1048576 + (assetNum % 9) * 1048576;
        metadata.setProperty("dam:size", size);
        
        // Last modified
        long timestamp = System.currentTimeMillis() - (assetNum * 1000L);
        Calendar metaCal = Calendar.getInstance();
        metaCal.setTimeInMillis(timestamp);
        metadata.setProperty("jcr:lastModified", ISO8601.format(metaCal));
        
        // SHA1 hash (for deduplication testing)
        metadata.setProperty("dam:sha1", generateSha1(assetNum));
        
        // Create renditions folder [include3]
        Tree renditions = jcrContent.addChild("renditions");
        renditions.setProperty("jcr:primaryType", "nt:folder", Type.NAME);
        
        // Create original rendition [include4, include5]
        createRendition(renditions, "original", "image/jpeg");
        
        // Create thumbnail rendition
        createRendition(renditions, "thumbnail", "image/jpeg");
        
        // Create extracted text rendition [include10]
        createTextRendition(renditions, assetNum);
        
        // Create comments [include6, include7]
        Tree comments = jcrContent.addChild("comments");
        comments.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        
        // Add a comment (every 3rd asset)
        if (assetNum % 3 == 0) {
            Tree comment1 = comments.addChild("comment1");
            comment1.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
            comment1.setProperty("text", "Approved for use");
            comment1.setProperty("author", "admin");
            Calendar commentCal = Calendar.getInstance();
            comment1.setProperty("date", ISO8601.format(commentCal));
        }
        
        // Create data/master for content fragments [include8]
        // (only for certain asset types)
        if (assetNum % 5 == 0) {
            Tree data = jcrContent.addChild("data");
            data.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
            
            Tree master = data.addChild("master");
            master.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
            master.setProperty("contentFragment", true);
            master.setProperty("cq:model", "/conf/sample/settings/dam/cfm/models/article");
        }
        
        // Create usages [include9]
        Tree usages = jcrContent.addChild("usages");
        usages.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        usages.setProperty("usedBy", Arrays.asList("/content/site/page" + (assetNum % 10)), Type.STRINGS);
        usages.setProperty("dam:score", (long) (assetNum % 100));
    }
    
    private static void createRendition(Tree parent, String name, String mimeType) {
        Tree rendition = parent.addChild(name);
        rendition.setProperty("jcr:primaryType", "nt:file", Type.NAME);
        
        Tree jcrContent = rendition.addChild("jcr:content");
        jcrContent.setProperty("jcr:primaryType", "nt:resource", Type.NAME);
        jcrContent.setProperty("jcr:mimeType", mimeType);
        Calendar rendCal = Calendar.getInstance();
        jcrContent.setProperty("jcr:lastModified", ISO8601.format(rendCal));
    }
    
    private static void createTextRendition(Tree parent, int assetNum) {
        Tree textRendition = parent.addChild("cqdam.text.txt");
        textRendition.setProperty("jcr:primaryType", "nt:file", Type.NAME);
        
        Tree jcrContent = textRendition.addChild("jcr:content");
        jcrContent.setProperty("jcr:primaryType", "nt:resource", Type.NAME);
        jcrContent.setProperty("jcr:mimeType", "text/plain");
        Calendar textCal = Calendar.getInstance();
        jcrContent.setProperty("jcr:lastModified", ISO8601.format(textCal));
        
        // Simulated extracted text content
        String extractedText = "This is extracted text from asset " + assetNum + ". " +
                "Contains searchable content for fulltext queries. " +
                "Keywords: document, report, analysis, data, " + (assetNum % 2 == 0 ? "quarterly" : "annual");
        jcrContent.setProperty("jcr:data", extractedText);
    }
    
    private static Tree ensurePath(Root root, String path) {
        Tree current = root.getTree("/");
        
        String[] segments = path.split("/");
        for (String segment : segments) {
            if (segment.isEmpty()) continue;
            
            Tree child = current.getChild(segment);
            if (!child.exists()) {
                child = current.addChild(segment);
                child.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
            }
            current = child;
        }
        
        return current;
    }
    
    private static String getRandomTag() {
        String[] category = TAG_CATEGORIES[RANDOM.nextInt(TAG_CATEGORIES.length)];
        return category[RANDOM.nextInt(category.length)];
    }
    
    private static String generateSha1(int assetNum) {
        // Generate a pseudo-SHA1 hash (not cryptographically secure, just for testing)
        return String.format("%040x", (long) assetNum * 0x123456789ABCDEFL);
    }
    
    private static String[] getPropertyArray(Tree tree, String propertyName) {
        if (!tree.hasProperty(propertyName)) {
            return new String[0];
        }
        
        Iterable<String> values = tree.getProperty(propertyName).getValue(Type.STRINGS);
        java.util.List<String> list = new java.util.ArrayList<>();
        for (String value : values) {
            list.add(value);
        }
        return list.toArray(new String[0]);
    }
}

