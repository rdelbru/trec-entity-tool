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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;
import org.sindice.siren.analysis.TupleAnalyzer;
import org.sindice.siren.analysis.TupleAnalyzer.URINormalisation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Index a list of entities, creating incoming, outgoing triples fields, subject
 * and type fields. The type field is a grouping of the rdf:type objects for this
 * entity.<br>
 * Outgoing triples are stored as n-tuples where a predicate has all its related
 * values.
 * Incoming triples are also stored as n-tuples, the difference being that a
 * predicate possess its related subject URIs.
 */
public abstract class Indexing implements Iterator<Entity> {
  
  protected final Logger            logger            = LoggerFactory.getLogger(Indexing.class);
  
  /* Perform a commit by batch of COMMIT documents */
  public static int                 COMMIT            = 10000;
  
  // FIELDS
  final static public String        INCOMING_TRIPLE   = "incoming-triple";
  final static public String        OUTGOING_TRIPLE   = "outgoing-triple";
  final static public String        SUBJECT           = "subject";
  final static public String        TYPE              = "type";
  
  /* The dataset files */
  protected final File[]            input;
  protected int                     inputPos          = 0;
  /* The current reader into the compressed archive */
  protected TarArchiveInputStream   reader            = null;
  /* A file entry in the archive */
  protected TarArchiveEntry         tarEntry;
  
  /* SIREn index */
  protected final Directory         indexDir;
  protected final IndexWriter       writer;

  /**
   * Create a SIREn index at indexDir, taking the files at inputDir as input.
   * @param inputDir
   * @param dir
   * @throws IOException
   */
  public Indexing(final File inputDir, final Directory dir)
  throws IOException {
    this.input = inputDir.listFiles(new FilenameFilter() {
      
      @Override
      public boolean accept(File dir, String name) {
        return name.matches(getPattern());
      }
      
    });
    /*
     *  Sort by filename: important because in the SIndice-ED dataset, two
     *  consecutive dumps can store a same entity
     */
    Arrays.sort(this.input);
    if (this.input.length == 0) {
      throw new RuntimeException("No archive files in the folder: " + inputDir.getAbsolutePath());
    }
    this.indexDir = dir;
    this.writer = initializeIndexWriter(this.indexDir);
    reader = getTarInputStream(this.input[0]);
    logger.info("Creating index from input located at {} ({} files)", inputDir.getAbsolutePath(), input.length);
    logger.info("Reading dump: {}", this.input[0]);
  }
  
  /**
   * The regular expression of the input files
   * @return
   */
  protected abstract String getPattern();

  /**
   * Create a buffered tar inputstream from the file in
   * @param in
   * @return
   * @throws FileNotFoundException
   * @throws IOException
   */
  private TarArchiveInputStream getTarInputStream(final File in)
  throws FileNotFoundException, IOException {
    return new TarArchiveInputStream(new BufferedInputStream(new GZIPInputStream(new FileInputStream(in))));
  }
  
  @Override
  public boolean hasNext() {
    return this.hasNext(null);
  }
  
  /**
   * Move to the next tar entry.
   * @param rootDir an entry path
   * @return true if a next tar entry can be read, or if this entry name is a sub-folder of rootDir
   */
  protected boolean hasNext(final String rootDir) {
    try {
      /*
       * if reader.available() is not equal to 0, then it means that this entry
       * has been loaded, but not read.
       */
      while (reader.available() == 0 && (tarEntry = reader.getNextTarEntry()) == null) { // Next tar entry
        if (++inputPos >= input.length) {
          reader.close();
          return false;
        }
        // Next archive file
        reader.close();
        logger.info("Reading dump: {}", this.input[inputPos]);
        reader = getTarInputStream(input[inputPos]);
      }
    } catch (IOException e) {
      logger.error("Error while reading the input: {}\n{}", input[inputPos], e);
    }
    /*
     *  When returning from this method, the inputstream is positionned at a regular file,
     *  i.e., metadata, outgoing-triples.nt or incoming-triples.nt.
     */
    if (tarEntry.isDirectory()) {
      return hasNext(rootDir);
    }
    return rootDir == null || tarEntry.getName().startsWith(rootDir) ? true : false;
  }
  
  /**
   * Create a index writer that uses a #TupleAnalyzer on the triples fields with
   * a tokenization of the URI's localname, and the default #WhitespaceAnalyzer
   * on the others.
   * @param dir
   * @return
   * @throws IOException
   */
  private IndexWriter initializeIndexWriter(final Directory dir)
  throws IOException {
    final Analyzer defaultAnalyzer = new WhitespaceAnalyzer(Version.LUCENE_31);
    final Map<String, Analyzer> fieldAnalyzers = new HashMap<String, Analyzer>();
    final TupleAnalyzer tuple = new TupleAnalyzer(new StandardAnalyzer(Version.LUCENE_31));
    tuple.setURINormalisation(URINormalisation.LOCALNAME);
    fieldAnalyzers.put(OUTGOING_TRIPLE, tuple);
    fieldAnalyzers.put(INCOMING_TRIPLE, tuple);

    final IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_31, new PerFieldAnalyzerWrapper(defaultAnalyzer, fieldAnalyzers));
    
    // Disable compound file
    ((LogMergePolicy) config.getMergePolicy()).setUseCompoundFile(false);
    // Increase merge factor to 20 - more adapted to batch creation
    ((LogMergePolicy) config.getMergePolicy()).setMergeFactor(20);
    
    config.setRAMBufferSizeMB(256);
    config.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);
    config.setMaxBufferedDeleteTerms(IndexWriterConfig.DISABLE_AUTO_FLUSH);

    final IndexWriter writer = new IndexWriter(dir, config);
    writer.setMaxFieldLength(Integer.MAX_VALUE);
    return writer;
  }

  /**
   * Creates an entity index
   * @throws CorruptIndexException
   * @throws IOException
   */
  public void indexIt()
  throws CorruptIndexException, IOException {
    Entity entity = null;
    long counter = 0;
    
    while (hasNext()) { // for each entity
      entity = next();
      
      Document doc = new Document();
      doc.add(new Field(SUBJECT, entity.subject, Store.YES, Index.NO));
      doc.add(new Field(TYPE, Utils.toString(entity.type), Store.YES, Index.NO));
      doc.add(new Field(OUTGOING_TRIPLE, entity.getTriples(true), Store.YES, Index.ANALYZED_NO_NORMS));
      doc.add(new Field(INCOMING_TRIPLE, entity.getTriples(false), Store.YES, Index.ANALYZED_NO_NORMS));
      writer.addDocument(doc);
      counter = commit(true, counter, entity.subject);
    }
    commit(false, counter, entity.subject); // Commit what is left
    writer.optimize();
  }
  
  /**
   * Commits the documents by batch
   * @param indexing
   * @param counter
   * @param subject
   * @return
   * @throws CorruptIndexException
   * @throws IOException
   */
  private long commit(boolean indexing, long counter, String subject)
  throws CorruptIndexException, IOException {
    if (!indexing || (++counter % COMMIT) == 0) { // Index by batch
      writer.commit();
      logger.info("Commited {} entities. Last entity: {}", (indexing ? COMMIT : counter), subject);
    }
    return counter;
  }
  
  /**
   * Close resources
   * @throws CorruptIndexException
   * @throws IOException
   */
  public void close()
  throws CorruptIndexException, IOException {
    try {
      writer.close(); 
    } finally {
      indexDir.close();
    }
  }

  @Override
  public void remove() {
  }
  
}
