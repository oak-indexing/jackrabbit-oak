<!--
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
  -->

## Elasticsearch Index

Oak elasticsearch indexes are based on [Elasticsearch](https://www.elastic.co/elasticsearch/) indexes. The configuration
options are very similar to those present in lucene indexes with a few differences which are indicated in this document.

* [Configuring Elasticsearch Server Link](#configure-elastic-server)
* [Index Definition Changes](#index-definition-changes)
* [Similarity Search based on Feature Vectors](#similarity-search)

### <a name="configure-elastic-server"></a> Configuring Elasticsearch Server
The elasticsearch server related configuration options are present as osgi config. The configuration needs to be done for
PID `org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexProviderService`

### <a name="index-definition-changes"></a> Index Definition Changes

    Changed properties
    - type (string) = 'elasticsearch' mandatory    
    - async (string) = 'elastic-async' mandatory
      
    Added properties
    - similarityTagsEnabled (boolean) = true
    - similarityTagsBoost (double) = 0.5f
      
    Removed properties 
    - compatVersion (long)
    - codec (string)
    - refresh (boolean)
    
### <a name="similarity-search"></a> Similarity Search based on Feature Vectors
This feature is just like the feature vector based simiarity search available in lucene indexes and can be used for finding
similar binary content (e.g. images). The index definition should have a rule for property containing a feature vector with
the `useInSimilarity` parameter set to true.
#### Differences with Lucene Index
We are not using Locality Sensitive Hashing (LSH) based on MinHash (used in Lucene index) for similarity search in Elasticsearch. We are using
[stable distributions](https://www.mlpack.org/papers/lsh.pdf) method for LSH. The available configuration options on the property definition are -
    
    - queryModel (string) = 'lsh'
    - L (long) = 20
    - k (long) = 15
    - w (long) = 500
    - candidates (long) = 500
    - probes (long) = 3

queryModel
: the model to be used for searching. Could be one of 'lsh' or 'exact'. The value `lsh` would mean that the similarity
search would be performed using `lsh` approach while `exact` would mean that similarity search would be performed based on
exact Euclidean distances

L
: the value of parameter `L` for LSH. It indicates number of hash tables to be used

k
: the value of parameter `k` for LSH. It indicates number of projections per hash value

w
: the value of parameter 'w'/'r' for LSH. It indicates width of projection.

candidates
: We take the top vectors with the most matching hashes and compute their exact similarity to the query vector. The candidates parameter
controls the number of exact similarity computations. Candidates must be set to a number greater or equal to the number of Elasticsearch
results you want to get. Higher values generally mean higher recall and higher latency.

probes
: Number of probes for using the multiprobe search technique. Max value is 3^k. Generally, increasing probes will
increase recall, will allow you to use a smaller value for L with comparable recall, but introduces some additional computation at query time.



