/**
 * Copyright 2011, Campinas Stephane
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/**
 * @project trec-entity-tool
 * @author Campinas Stephane [ 3 Jun 2011 ]
 * @link stephane.campinas@deri.org
 */
package org.sindice.siren.index;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

/**
 * An entity of the dataset
 */
public class Entity {

  /* incoming-triples.nt */
  final HashMap<String, HashSet<String>> inTuples = new HashMap<String, HashSet<String>>();
  /* outgoing-triples.nt */
  final HashMap<String, HashSet<String>> outTuples = new HashMap<String, HashSet<String>>();
  /* metadata */
  final StringBuilder sbMetadata = new StringBuilder();
  /* rdf:type statement's objects */
  final HashSet<String> type = new HashSet<String>();
  
  final StringBuilder sb = new StringBuilder();
  
  String subject = ""; // The URI of the entity
  String context = ""; // The URL of the document where the entity is from
  
  public void clear() {
    subject = "";
    context = "";
    inTuples.clear();
    outTuples.clear();
    sb.setLength(0);
    type.clear();
    sbMetadata.setLength(0);
  }
  
  public String getTriples(boolean out) {
    final HashMap<String, HashSet<String>> map = out ? this.outTuples : this.inTuples;
    
    sb.setLength(0);
    for (Entry<String, HashSet<String>> e : map.entrySet()) {
      sb.append(e.getKey()).append(' ').append(Utils.toString(e.getValue()));
    }
    return sb.toString();
  }
  
}
