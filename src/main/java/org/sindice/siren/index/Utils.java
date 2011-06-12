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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

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
  private static final String         RDF_TYPE  = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";
  private static final StringBuilder  sb        = new StringBuilder();
  private static RDFParser            parser    = null;
  private static StatementCollector   collector = null;  

  // Efficient byte to char conversion
  private static final int BYTE_RANGE = (1 + Byte.MAX_VALUE) - Byte.MIN_VALUE;
  private static byte[] allBytes = new byte[BYTE_RANGE];
  private static char[] byteToChars = new char[BYTE_RANGE];

  static {
    for (int i = Byte.MIN_VALUE; i <= Byte.MAX_VALUE; i++) {
      allBytes[i - Byte.MIN_VALUE] = (byte) i;
    }
    String allBytesString = new String(allBytes, 0, Byte.MAX_VALUE - Byte.MIN_VALUE);
    for (int i = 0; i < (Byte.MAX_VALUE - Byte.MIN_VALUE); i++) {
      byteToChars[i] = allBytesString.charAt(i);
    }
  }
  
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
      toAsciiString(data, bbuffer.capacity());
      bbuffer.clear();
    }
    reader.read(bbuffer.array(), 0, (int) size);
    toAsciiString(data, (int) size);
  }

  /**
   * Convert the byte array in the platform encoding
   * @param data the string buffer
   * @param length number of bytes to decode
   */
  private static final void toAsciiString(final StringBuilder data, final int length) {
    for (int i = 0; i < length; i++) {
      data.append(byteToChars[(int) bbuffer.get(i) - Byte.MIN_VALUE]);
    }
  }
  
  /**
   * Sort and flatten a list of triples to n-tuples containing many objects for
   * the same predicate. Generate one n-tuple per predicate.
   * The tuples are ordered by predicate. <br>
   * The sorted and flatten representation is generally more efficient in term
   * of index size than the normal flatten approach.
   * 
   * @param triples
   * @param map
   * @param types
   * @param isOut
   */
  public static void sortAndFlattenNTriples(final StringBuilder triples, final HashMap<String, HashSet<String>> map, final HashSet<String> types, final boolean isOut) {
    flattenNTriples(triples, map, types, isOut);
  }
  
  private static void initParser() {
    if (parser == null) {
      parser = (RDFParser) new NTriplesParser();
      collector = new StatementCollector();
      parser.setRDFHandler(collector);
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
  private static void flattenNTriples(final StringBuilder triples, final Map<String, HashSet<String>> map, final HashSet<String> types, final boolean isOut) {
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
          types.add(object);
        } else {
          HashSet<String> hs = map.get(predicate);
          final String toAdd = isOut ? object : subject;
          if (hs == null) {
            hs = new HashSet<String>();
            map.put(predicate, hs);
          }
          if (hs.size() < 65535) // 2 ^ 16 - 1
            hs.add(toAdd);
        }
      }
    } catch (RDFParseException e1) {
    } catch (RDFHandlerException e1) {
    } catch (IOException e1) {
    }
    triples.setLength(0);
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
