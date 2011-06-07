/**
 * Copyright 2011, Campinas Stephane Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law
 * or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
/**
 * @project trec-entity-tool
 * @author Campinas Stephane [ 3 Jun 2011 ]
 * @link stephane.campinas@deri.org
 */
package org.sindice.siren.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;

/**
 * Helper functions for indexing
 */
public class Utils {

  /* byte array used for reading the compressed tar files */
  private static final ByteBuffer     bbuffer   = ByteBuffer.allocate(1024);
  private static final String         RDF_TYPE  = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";
  private static final StringBuilder  sb        = new StringBuilder();
  
  /**
   * Read size bytes from the reader at the current position
   * 
   * @param reader
   *          the TarArchiveInputStream reader
   * @param size
   *          the number of bytes to read
   * @param data
   *          the buffer to store the content
   * @throws IOException
   */
  public static void getFile(final TarArchiveInputStream reader, long size, final StringBuilder data)
  throws IOException {
    bbuffer.clear();
    while (size > bbuffer.capacity()) {
      reader.read(bbuffer.array(), 0, bbuffer.capacity());
      size -= bbuffer.capacity();
      data.append(new String(bbuffer.array(), 0, bbuffer.capacity()));
      bbuffer.clear();
    }
    reader.read(bbuffer.array(), 0, (int) size);
    data.append(new String(bbuffer.array(), 0, (int) size));
  }

  /**
   * Sort and flatten a list of triples to n-tuples containing many objects for
   * the same predicate. Generate one n-tuple per predicate.
   * The tuples are ordered by predicate. <br>
   * The sorted and flatten representation is generally more efficient in term
   * of index size than the normal flatten approach.
   * 
   * @param values
   *          The list of n-triples.
   * @return The n-tuples concatenated.
   */
  public static void sortAndFlattenNTriples(final StringBuilder triples, final HashSet<String> types, final boolean isOut) {
    final Map<String, StringBuilder> map = new TreeMap<String, StringBuilder>();
    flattenNTriples(triples, map, types, isOut);
  }

  /**
   * Flatten a list of triples to n-tuples containing many objects for the same
   * predicate. Generate one n-tuple per predicate.
   * 
   * @param values
   *          The list of n-triples.
   * @return The n-tuples concatenated.
   */
  private static void flattenNTriples(final StringBuilder triples, final Map<String, StringBuilder> map, final HashSet<String> types, final boolean isOut) {
    for (int i = 0, j = 0; i < triples.length(); i++) {
      if (triples.charAt(i) == '\n') { // for each triple
        final String value = triples.substring(j, i);
        j = i + 1;
        
        final int firstWhitespace = value.indexOf(' ');
        final int secondWhitespace = value.indexOf(' ', firstWhitespace + 1);
        final int lastDot = value.lastIndexOf('.');
        if (firstWhitespace == -1 || secondWhitespace == -1 || lastDot == -1) {
          continue; // probably invalid triple, just skip it
        }
        final String predicate = value.substring(firstWhitespace + 1, secondWhitespace);
        if (types != null && predicate.equals(RDF_TYPE)) {
          types.add(value.substring(secondWhitespace + 1, lastDot - 1));
        } else {
          final String object = isOut ? value.substring(secondWhitespace + 1, lastDot) : value.substring(0, firstWhitespace + 1);
          StringBuilder tb = map.get(predicate);
          if (tb == null) {
            tb = new StringBuilder();
            map.put(predicate, tb);
          }
          tb.append(object);          
        }
      }
    }
    // Replace the string buffer with the flattened triples.
    triples.setLength(0);
    for (Entry<String, StringBuilder> t : map.entrySet()) {
      triples.append(t.getKey()).append(" ").append(t.getValue()).append(".\n");
    }
  }
  
  /**
   * Outputs elements of the hashset into a string, separated by a whitespace
   * and ending with a dot.
   * @param set
   * @return
   */
  public static String toString(final HashSet<String> set) {
    sb.setLength(0);
    for (String s : set) {
      sb.append(s).append(' ');
    }
    sb.append(".\n");
    return sb.toString();
  }
  
}
