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
 * @author Campinas Stephane [ 5 Jun 2011 ]
 * @link stephane.campinas@deri.org
 */
package org.sindice.siren.trec;

import static org.junit.Assert.assertEquals;

import java.io.File;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Test;
import org.sindice.siren.index.Indexing;
import org.sindice.siren.index.SindiceEDIndexing;
import org.sindice.siren.search.SirenCellQuery;
import org.sindice.siren.search.SirenTermQuery;
import org.sindice.siren.search.SirenTupleQuery;
import org.sindice.siren.search.SirenTupleClause.Occur;


/**
 * 
 */
public class SindiceEDIndexingTest {

  private final Term outgoingField = new Term(Indexing.OUTGOING_TRIPLE);
  
  @Test
  public void testSimpleQueries()
  throws Exception {
    final Directory dir = new RAMDirectory();
    final File input = new File("./src/test/resources");
    final SindiceEDIndexing indexED = new SindiceEDIndexing(input, dir);
    indexED.indexIt();
    
    final IndexSearcher searcher = new IndexSearcher(dir);
    
    // Search for any entities where the currency is in USD
    SirenTupleQuery tq = new SirenTupleQuery();
    SirenCellQuery cell = new SirenCellQuery(new SirenTermQuery(outgoingField.createTerm("hascurrency")));
    cell.setConstraint(0);
    tq.add(cell, Occur.MUST);
    cell = new SirenCellQuery(new SirenTermQuery(outgoingField.createTerm("usd")));
    tq.add(cell, Occur.MUST);
    TopDocs td = searcher.search(tq, 10);
    assertEquals(44, td.totalHits);
    dir.close();
  }
  
}
