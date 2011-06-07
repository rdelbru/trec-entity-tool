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
package org.sindice.siren.trec;

import static org.junit.Assert.assertEquals;

import java.io.File;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Test;
import org.sindice.siren.index.Indexing;
import org.sindice.siren.index.SindiceDEIndexing;
import org.sindice.siren.search.SirenCellQuery;
import org.sindice.siren.search.SirenTermQuery;
import org.sindice.siren.search.SirenTupleQuery;
import org.sindice.siren.search.SirenTupleClause.Occur;


/**
 * 
 */
public class SindiceDEIndexingTest {

  private final Term outgoingField = new Term(Indexing.OUTGOING_TRIPLE);
  
  @Test
  public void testSimpleQueries()
  throws Exception {
    final Directory dir = new RAMDirectory();
    final File input = new File("./src/test/resources");
    final SindiceDEIndexing indexDE = new SindiceDEIndexing(input, dir);
    indexDE.indexIt();

    final IndexSearcher searcher = new IndexSearcher(dir);
    
    // Search for any entities containing the term "rna"
    TopDocs td = searcher.search(new SirenTermQuery(outgoingField.createTerm("rna")), 10);
    assertEquals(1, td.totalHits);
    final Document doc = searcher.getIndexReader().document(td.scoreDocs[0].doc);
    assertEquals("http://eprints.rkbexplorer.com/id/caltech/eprints-7519", doc.get(Indexing.SUBJECT));
    
    // Search for any entities that have ther term "abstract" in the predicate (i.e., the cell 0 of the tuple).
    final SirenTupleQuery tq = new SirenTupleQuery();
    final SirenCellQuery cell = new SirenCellQuery(new SirenTermQuery(outgoingField.createTerm("abstract")));
    cell.setConstraint(0);
    tq.add(cell, Occur.MUST);
    td = searcher.search(tq, 10);
    assertEquals(2, td.totalHits);
    
    dir.close();
  }
  
}
