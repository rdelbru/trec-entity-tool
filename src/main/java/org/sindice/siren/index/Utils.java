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
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.helpers.StatementCollector;
import org.openrdf.rio.ntriples.NTriplesParser;

/**
 * Helper functions for indexing
 */
public class Utils {

  /* byte array used for reading the compressed tar files */
  private static final ByteBuffer     bbuffer   = ByteBuffer.allocate(1024);
  private static final String         RDF_TYPE  = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
  private static final StringBuilder  sb        = new StringBuilder();
  private static RDFParser            parser    = null;
  private static StatementCollector   collector = null;
  
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
  
  private static void initParser() {
    if (parser == null) {
      parser = (RDFParser) new NTriplesParser();
      collector = new StatementCollector();
      parser.setRDFHandler(collector);
      parser.setVerifyData(false);
      parser.setStopAtFirstError(false);
    }
    collector.clear();
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
    try {
      initParser();
      parser.parse(new StringReader(triples.toString()), "");
      for (Statement st : collector.getStatements()) {
        sb.setLength(0);
        final String subject = sb.append('<').append(st.getSubject().toString()).append('>').toString();
        sb.setLength(0);
        final String predicate = sb.append('<').append(st.getPredicate().toString()).append('>').toString();
        sb.setLength(0);
        final String object = (st.getObject() instanceof URI) ? sb.append('<').append(st.getObject().toString()).append('>').toString()
                                                              : st.getObject().toString();
        if (types != null && predicate.equals(RDF_TYPE)) {
          types.add(st.getObject().toString());
        } else {
          StringBuilder tb = map.get(predicate);
          if (tb == null) {
            tb = new StringBuilder();
            map.put(predicate, tb);
          }
          tb.append(isOut ? object : subject);
        }
      }
    } catch (RDFParseException e1) {
    } catch (RDFHandlerException e1) {
    } catch (IOException e1) {
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
