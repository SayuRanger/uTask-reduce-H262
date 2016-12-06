/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.SortedRanges.SkipRangeIterator;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormatCounter;
import org.apache.hadoop.mapreduce.task.reduce.Shuffle;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

/** A Reduce task. */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ReduceTask extends Task {

  static {                                        // register a ctor
    WritableFactories.setFactory
      (ReduceTask.class,
       new WritableFactory() {
         public Writable newInstance() { return new ReduceTask(); }
       });
  }
  
  private static final Log LOG = LogFactory.getLog(ReduceTask.class.getName());
  private int numMaps;

  private CompressionCodec codec;

  //Following variables are used to support uTask.
  private int previousPartitionID;
  private int newFileStartPartitionID;
  private boolean isSort;
  private int keyCount;
  private int reduceID;
  private boolean isContention;

  // If this is a LocalJobRunner-based job, this will
  // be a mapping from map task attempts to their output files.
  // This will be null in other cases.
  private Map<TaskAttemptID, MapOutputFile> localMapFiles;

  { 
    getProgress().setStatus("reduce"); 
    setPhase(TaskStatus.Phase.SHUFFLE);        // phase to start with 
  }

  private Progress copyPhase;
  private Progress sortPhase;
  private Progress reducePhase;
  private Counters.Counter shuffledMapsCounter = 
    getCounters().findCounter(TaskCounter.SHUFFLED_MAPS);
  private Counters.Counter reduceShuffleBytes = 
    getCounters().findCounter(TaskCounter.REDUCE_SHUFFLE_BYTES);
  private Counters.Counter reduceInputKeyCounter = 
    getCounters().findCounter(TaskCounter.REDUCE_INPUT_GROUPS);
  private Counters.Counter reduceInputValueCounter = 
    getCounters().findCounter(TaskCounter.REDUCE_INPUT_RECORDS);
  private Counters.Counter reduceOutputCounter = 
    getCounters().findCounter(TaskCounter.REDUCE_OUTPUT_RECORDS);
  private Counters.Counter reduceCombineInputCounter =
    getCounters().findCounter(TaskCounter.COMBINE_INPUT_RECORDS);
  private Counters.Counter reduceCombineOutputCounter =
    getCounters().findCounter(TaskCounter.COMBINE_OUTPUT_RECORDS);
  private Counters.Counter fileOutputByteCounter =
    getCounters().findCounter(FileOutputFormatCounter.BYTES_WRITTEN);

  // A custom comparator for map output files. Here the ordering is determined
  // by the file's size and path. In case of files with same size and different
  // file paths, the first parameter is considered smaller than the second one.
  // In case of files with same size and path are considered equal.
  private Comparator<FileStatus> mapOutputFileComparator = 
      new Comparator<FileStatus>() {
    public int compare(FileStatus a, FileStatus b) {
      if (a.getLen() < b.getLen())
        return -1;
      else if (a.getLen() == b.getLen())
        if (a.getPath().toString().equals(b.getPath().toString()))
          return 0;
        else
          return -1; 
      else
        return 1;
    }
  };

  // A sorted set for keeping a set of map output files on disk
  private final SortedSet<FileStatus> mapOutputFilesOnDisk = 
      new TreeSet<FileStatus>(mapOutputFileComparator);
  
  public ReduceTask() {
    super();
  }

  public ReduceTask(String jobFile, TaskAttemptID taskId,
                    int partition, int numMaps, int numSlotsRequired) {
    super(jobFile, taskId, partition, numSlotsRequired);
    this.numMaps = numMaps;
  }
  

  /**
   * Register the set of mapper outputs created by a LocalJobRunner-based
   * job with this ReduceTask so it knows where to fetch from.
   *
   * This should not be called in normal (networked) execution.
   */
  public void setLocalMapFiles(Map<TaskAttemptID, MapOutputFile> mapFiles) {
    this.localMapFiles = mapFiles;
  }

  private CompressionCodec initCodec() {
    // check if map-outputs are to be compressed
    if (conf.getCompressMapOutput()) {
      Class<? extends CompressionCodec> codecClass =
        conf.getMapOutputCompressorClass(DefaultCodec.class);
      return ReflectionUtils.newInstance(codecClass, conf);
    } 

    return null;
  }

  @Override
  public boolean isMapTask() {
    return false;
  }

  public int getNumMaps() { return numMaps; }
  
  /**
   * Localize the given JobConf to be specific for this task.
   */
  @Override
  public void localizeConfiguration(JobConf conf) throws IOException {
    super.localizeConfiguration(conf);
    conf.setNumMapTasks(numMaps);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);

    out.writeInt(numMaps);                        // write the number of maps
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);

    numMaps = in.readInt();
  }
  
  // Get the input files for the reducer (for local jobs).
  private Path[] getMapFiles(FileSystem fs) throws IOException {
    List<Path> fileList = new ArrayList<Path>();
    for(int i = 0; i < numMaps; ++i) {
      fileList.add(mapOutputFile.getInputFile(i));
    }
    return fileList.toArray(new Path[0]);
  }

  private class ReduceValuesIterator<KEY,VALUE> 
          extends ValuesIterator<KEY,VALUE> {
    public ReduceValuesIterator (RawKeyValueIterator in,
                                 RawComparator<KEY> comparator, 
                                 Class<KEY> keyClass,
                                 Class<VALUE> valClass,
                                 Configuration conf, Progressable reporter)
      throws IOException {
      super(in, comparator, keyClass, valClass, conf, reporter);
    }

    @Override
    public VALUE next() {
      reduceInputValueCounter.increment(1);
      return moveToNext();
    }
    
    protected VALUE moveToNext() {
      return super.next();
    }
    
    public void informReduceProgress() {
      reducePhase.set(super.in.getProgress().getProgress()); // update progress
      reporter.progress();
    }
  }

  private class SkippingReduceValuesIterator<KEY,VALUE> 
     extends ReduceValuesIterator<KEY,VALUE> {
     private SkipRangeIterator skipIt;
     private TaskUmbilicalProtocol umbilical;
     private Counters.Counter skipGroupCounter;
     private Counters.Counter skipRecCounter;
     private long grpIndex = -1;
     private Class<KEY> keyClass;
     private Class<VALUE> valClass;
     private SequenceFile.Writer skipWriter;
     private boolean toWriteSkipRecs;
     private boolean hasNext;
     private TaskReporter reporter;
     
     public SkippingReduceValuesIterator(RawKeyValueIterator in,
         RawComparator<KEY> comparator, Class<KEY> keyClass,
         Class<VALUE> valClass, Configuration conf, TaskReporter reporter,
         TaskUmbilicalProtocol umbilical) throws IOException {
       super(in, comparator, keyClass, valClass, conf, reporter);
       this.umbilical = umbilical;
       this.skipGroupCounter = 
         reporter.getCounter(TaskCounter.REDUCE_SKIPPED_GROUPS);
       this.skipRecCounter = 
         reporter.getCounter(TaskCounter.REDUCE_SKIPPED_RECORDS);
       this.toWriteSkipRecs = toWriteSkipRecs() &&  
         SkipBadRecords.getSkipOutputPath(conf)!=null;
       this.keyClass = keyClass;
       this.valClass = valClass;
       this.reporter = reporter;
       skipIt = getSkipRanges().skipRangeIterator();
       mayBeSkip();
     }
     
     public void nextKey() throws IOException {
       super.nextKey();
       mayBeSkip();
     }
     
     public boolean more() { 
       return super.more() && hasNext; 
     }
     
     private void mayBeSkip() throws IOException {
       hasNext = skipIt.hasNext();
       if(!hasNext) {
         LOG.warn("Further groups got skipped.");
         return;
       }
       grpIndex++;
       long nextGrpIndex = skipIt.next();
       long skip = 0;
       long skipRec = 0;
       while(grpIndex<nextGrpIndex && super.more()) {
         while (hasNext()) {
           VALUE value = moveToNext();
           if(toWriteSkipRecs) {
             writeSkippedRec(getKey(), value);
           }
           skipRec++;
         }
         super.nextKey();
         grpIndex++;
         skip++;
       }
       
       //close the skip writer once all the ranges are skipped
       if(skip>0 && skipIt.skippedAllRanges() && skipWriter!=null) {
         skipWriter.close();
       }
       skipGroupCounter.increment(skip);
       skipRecCounter.increment(skipRec);
       reportNextRecordRange(umbilical, grpIndex);
     }
     
     @SuppressWarnings("unchecked")
     private void writeSkippedRec(KEY key, VALUE value) throws IOException{
       if(skipWriter==null) {
         Path skipDir = SkipBadRecords.getSkipOutputPath(conf);
         Path skipFile = new Path(skipDir, getTaskID().toString());
         skipWriter = SequenceFile.createWriter(
               skipFile.getFileSystem(conf), conf, skipFile,
               keyClass, valClass, 
               CompressionType.BLOCK, reporter);
       }
       skipWriter.append(key, value);
     }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void run(JobConf job, final TaskUmbilicalProtocol umbilical)
    throws IOException, InterruptedException, ClassNotFoundException {
    keyCount =0;
    reduceID = getTaskID().getTaskID().getId();
    run2(job,umbilical);
  }

  @SuppressWarnings("unchecked")
  private <INKEY,INVALUE,OUTKEY,OUTVALUE>
  void run2(JobConf job,final TaskUmbilicalProtocol umbilical)
    throws IOException, InterruptedException,ClassNotFoundException{
    job.setBoolean(JobContext.SKIP_RECORDS, isSkipping());

    if (isMapOrReduce()) {
      for(int i =0;i<job.getNumReduceTasks();i++) {
        copyPhase = getProgress().addPhase("copy");
        sortPhase = getProgress().addPhase("sort");
        reducePhase = getProgress().addPhase("reduce");
      }
    }
    // start thread that will handle communication with parent
    TaskReporter reporter = startReporter(umbilical);

    boolean useNewApi = job.getUseNewReducer();
    initialize(job, getJobID(), reporter, useNewApi);

    // check if it is a cleanupJobTask
    if (jobCleanup) {
      runJobCleanupTask(umbilical, reporter);
      return;
    }
    if (jobSetup) {
      runJobSetupTask(umbilical, reporter);
      return;
    }
    if (taskCleanup) {
      runTaskCleanupTask(umbilical, reporter);
      return;
    }

    // Initialize the codec
    codec = initCodec();
    RawKeyValueIterator rIter = null;
    ShuffleConsumerPlugin shuffleConsumerPlugin = null;

    Class combinerClass = conf.getCombinerClass();
    CombineOutputCollector combineCollector =
            (null != combinerClass) ?
                    new CombineOutputCollector(reduceCombineOutputCounter, reporter, conf) : null;

    Class<? extends ShuffleConsumerPlugin> clazz =
            job.getClass(MRConfig.SHUFFLE_CONSUMER_PLUGIN, Shuffle.class, ShuffleConsumerPlugin.class);

    shuffleConsumerPlugin = ReflectionUtils.newInstance(clazz, job);
    LOG.info("Using ShuffleConsumerPlugin: " + shuffleConsumerPlugin);

    boolean gyf_solution = getConf().getBoolean("gyf.reduce.solution",false);
    isSort = getConf().getBoolean("gyf.reduce.isSort",false);
    if(gyf_solution){

      ReduceProgressMessage request = new ReduceProgressMessage("request");
      request.setPartitionId(-1);
      request.setReduceId(reduceID);
      ReduceProgressMessage respond;

      long tt1 = System.currentTimeMillis();
      respond = umbilical.RPMProcessing(request);
      long tt2 = System.currentTimeMillis();
      LOG.info("[gyf.solution.rpc.call.time]: "+ (tt2 - tt1));

      //get start partition ID here.
      newFileStartPartitionID = respond.getPartitionId();
      previousPartitionID = -1;
      int partitionID;

      TaskID fTID = getTaskID().getTaskID();
      TaskID firstOutputTaskID = new TaskID(fTID.getJobID(),fTID.getTaskType(),newFileStartPartitionID);
      TaskAttemptID firstTAID = new TaskAttemptID(firstOutputTaskID,getTaskID().getId());

      org.apache.hadoop.mapreduce.TaskAttemptContext taskContext1 =
              new org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl(job,firstTAID,reporter);

      org.apache.hadoop.mapreduce.Reducer<INKEY,INVALUE,OUTKEY,OUTVALUE> reducer =
              (org.apache.hadoop.mapreduce.Reducer<INKEY,INVALUE,OUTKEY,OUTVALUE>)
                      ReflectionUtils.newInstance(taskContext1.getReducerClass(), job);

      org.apache.hadoop.mapreduce.RecordWriter<OUTKEY,OUTVALUE> trackedRW =
              new NewTrackingRecordWriter<OUTKEY, OUTVALUE>(this, taskContext1);

      while(respond.getPartitionId()!=-1){
        partitionID = respond.getPartitionId();

        //get current partition ID here.
        copyPhase = getProgress().addPhase("copy");
        int outputLocation=0;
        if(isSort){
          if(previousPartitionID == -1){
            outputLocation = newFileStartPartitionID;
          }
          else{
            if(previousPartitionID==partitionID-1){ //successive
              outputLocation = newFileStartPartitionID;
            }
            else{ //not successive
              outputLocation = partitionID;
              newFileStartPartitionID = partitionID;
            }
          }
        }
        else{
          outputLocation = newFileStartPartitionID;
        }
        TaskID TID = getTaskID().getTaskID();
        LOG.info("gyf.TID:"+TID);
        TaskID newID = new TaskID(TID.getJobID(),TID.getTaskType(),partitionID);
        LOG.info("gyf.newID:"+newID);
        TaskAttemptID newAttempt = new TaskAttemptID(newID,0);
        LOG.info("gyf.newAttempt"+newAttempt);
        long sct1 = System.currentTimeMillis();
        ShuffleConsumerPlugin.Context shuffleContext =
                new ShuffleConsumerPlugin.Context(newAttempt,
                        job, FileSystem.getLocal(job), umbilical,
                        super.lDirAlloc, reporter, codec,
                        combinerClass, combineCollector,
                        spilledRecordsCounter, reduceCombineInputCounter,
                        shuffledMapsCounter,
                        reduceShuffleBytes, failedShuffleCounter,
                        mergedMapOutputsCounter,
                        taskStatus, copyPhase, sortPhase, this,
                        mapOutputFile, localMapFiles);
        long sct2 = System.currentTimeMillis();
        LOG.info("[gyf.reduce.Shuffle.Context.Create.time]: " + (sct2-sct1) );

        long sit1 = System.currentTimeMillis();
        shuffleConsumerPlugin.init(shuffleContext);
        long sit2 = System.currentTimeMillis();
        LOG.info("[gyf.reduce.Shuffle.init.time]: " + (sit2 - sit1) );
        //long srt1 = System.currentTimeMillis();

        long srt1 = System.currentTimeMillis();
        rIter = shuffleConsumerPlugin.run();
        long srt2 = System.currentTimeMillis();
        LOG.info("[gyf.reduce.Shuffle.run.time]: " + (srt2 - srt1) );
        //long srt2 = System.currentTimeMillis();
        //LOG.info("[gyf.solution.shuffle.run]: "+ (srt2 - srt1));
        mapOutputFilesOnDisk.clear();

        sortPhase.complete();                         // sort is complete
        setPhase(TaskStatus.Phase.REDUCE);
        statusUpdate(umbilical);
        Class keyClass = job.getMapOutputKeyClass();
        Class valueClass = job.getMapOutputValueClass();
        RawComparator comparator = job.getOutputValueGroupingComparator();

        if (useNewApi) {
          isContention = (reduceID==1)?true:false;
          long rnt1 = System.currentTimeMillis();
          runNewReducer(job, umbilical, reporter, rIter, comparator,      //partitions output to different files according
                  keyClass, valueClass,                                   //to newAttempt.
                  newAttempt,isSort,taskContext1,reducer,trackedRW,isContention);
          long rnt2 = System.currentTimeMillis();
          LOG.info("[gyf.solution.run.NewReducer]: "+ (rnt2 - rnt1));
          //runNewReducer(job, umbilical, reporter, rIter, comparator,    //all partition output to one file.
          //        keyClass, valueClass);
        } else {
          runOldReducer(job, umbilical, reporter, rIter, comparator,
                  keyClass, valueClass);
        }
        request.setPartitionId(partitionID);
        previousPartitionID = partitionID;
        long t1 = System.currentTimeMillis();
        respond = umbilical.RPMProcessing(request);
        long t2 = System.currentTimeMillis();
        LOG.info("[gyf.solution.rpc.call.time]: "+ (t2 - t1));
      }
      if(isSort==false){
        Class keyClass = job.getMapOutputKeyClass();
        Class valueClass = job.getMapOutputValueClass();
        RawComparator comparator = job.getOutputValueGroupingComparator();
        org.apache.hadoop.mapreduce.Reducer.Context
              reducerContext = createReduceContext(reducer, job, firstTAID,
              rIter, reduceInputKeyCounter,
              reduceInputValueCounter,
              trackedRW,
              committer,
              reporter, comparator, keyClass,
              valueClass);
        long t1 =System.currentTimeMillis();
        trackedRW.close(reducerContext);
        long t2 = System.currentTimeMillis();
        LOG.info("gyf.runNewReducer.trackedRW.close: " + (t2-t1) );
      }
    }
    else{
      //run normal reduce.
      ShuffleConsumerPlugin.Context shuffleContext =
              new ShuffleConsumerPlugin.Context(getTaskID(), job, FileSystem.getLocal(job), umbilical,
                      super.lDirAlloc, reporter, codec,
                      combinerClass, combineCollector,
                      spilledRecordsCounter, reduceCombineInputCounter,
                      shuffledMapsCounter,
                      reduceShuffleBytes, failedShuffleCounter,
                      mergedMapOutputsCounter,
                      taskStatus, copyPhase, sortPhase, this,
                      mapOutputFile, localMapFiles);
      shuffleConsumerPlugin.init(shuffleContext);
      //LOG.info("shuffleConsumerPlugin starts to run.");
      long srt1 = System.currentTimeMillis();
      rIter = shuffleConsumerPlugin.run();
      long srt2 = System.currentTimeMillis();
      LOG.info("[gyf.reduce.Shuffle.run.time]: " + (srt2 - srt1) );

      mapOutputFilesOnDisk.clear();

      sortPhase.complete();                         // sort is complete
      setPhase(TaskStatus.Phase.REDUCE);
      statusUpdate(umbilical);
      Class keyClass = job.getMapOutputKeyClass();
      Class valueClass = job.getMapOutputValueClass();
      RawComparator comparator = job.getOutputValueGroupingComparator();

      if (useNewApi) {
        isContention = (reduceID==1)&&(getTaskID().getId()==0)?true:false;
        long rnt1 = System.currentTimeMillis();
        runNewReducer(job, umbilical, reporter, rIter, comparator,
                keyClass, valueClass,isContention);
        long rnt2 = System.currentTimeMillis();
        LOG.info("[gyf.solution.run.NewReducer]: "+ (rnt2 - rnt1));
      } else {
        runOldReducer(job, umbilical, reporter, rIter, comparator,
                keyClass, valueClass);
      }
    }
    shuffleConsumerPlugin.close();
    done(umbilical, reporter);
  }

  @SuppressWarnings("unchecked")
  private <INKEY,INVALUE,OUTKEY,OUTVALUE>
  void runOldReducer(JobConf job,
                     TaskUmbilicalProtocol umbilical,
                     final TaskReporter reporter,
                     RawKeyValueIterator rIter,
                     RawComparator<INKEY> comparator,
                     Class<INKEY> keyClass,
                     Class<INVALUE> valueClass) throws IOException {
    Reducer<INKEY,INVALUE,OUTKEY,OUTVALUE> reducer = 
      ReflectionUtils.newInstance(job.getReducerClass(), job);
    // make output collector
    String finalName = getOutputName(getPartition());

    RecordWriter<OUTKEY, OUTVALUE> out = new OldTrackingRecordWriter<OUTKEY, OUTVALUE>(
        this, job, reporter, finalName);
    final RecordWriter<OUTKEY, OUTVALUE> finalOut = out;
    
    OutputCollector<OUTKEY,OUTVALUE> collector = 
      new OutputCollector<OUTKEY,OUTVALUE>() {
        public void collect(OUTKEY key, OUTVALUE value)
          throws IOException {
          finalOut.write(key, value);
          // indicate that progress update needs to be sent
          reporter.progress();
        }
      };
    
    // apply reduce function
    try {
      //increment processed counter only if skipping feature is enabled
      boolean incrProcCount = SkipBadRecords.getReducerMaxSkipGroups(job)>0 &&
        SkipBadRecords.getAutoIncrReducerProcCount(job);
      
      ReduceValuesIterator<INKEY,INVALUE> values = isSkipping() ? 
          new SkippingReduceValuesIterator<INKEY,INVALUE>(rIter, 
              comparator, keyClass, valueClass, 
              job, reporter, umbilical) :
          new ReduceValuesIterator<INKEY,INVALUE>(rIter, 
          job.getOutputValueGroupingComparator(), keyClass, valueClass, 
          job, reporter);
      values.informReduceProgress();
      while (values.more()) {
        reduceInputKeyCounter.increment(1);
        reducer.reduce(values.getKey(), values, collector, reporter);
        if(incrProcCount) {
          reporter.incrCounter(SkipBadRecords.COUNTER_GROUP, 
              SkipBadRecords.COUNTER_REDUCE_PROCESSED_GROUPS, 1);
        }
        values.nextKey();
        values.informReduceProgress();
      }

      reducer.close();
      reducer = null;
      
      out.close(reporter);
      out = null;
    } finally {
      IOUtils.cleanup(LOG, reducer);
      closeQuietly(out, reporter);
    }
  }

  static class OldTrackingRecordWriter<K, V> implements RecordWriter<K, V> {

    private final RecordWriter<K, V> real;
    private final org.apache.hadoop.mapred.Counters.Counter reduceOutputCounter;
    private final org.apache.hadoop.mapred.Counters.Counter fileOutputByteCounter;
    private final List<Statistics> fsStats;

    @SuppressWarnings({ "deprecation", "unchecked" })
    public OldTrackingRecordWriter(ReduceTask reduce, JobConf job,
        TaskReporter reporter, String finalName) throws IOException {
      this.reduceOutputCounter = reduce.reduceOutputCounter;
      this.fileOutputByteCounter = reduce.fileOutputByteCounter;
      List<Statistics> matchedStats = null;
      if (job.getOutputFormat() instanceof FileOutputFormat) {
        matchedStats = getFsStatistics(FileOutputFormat.getOutputPath(job), job);
      }
      fsStats = matchedStats;

      FileSystem fs = FileSystem.get(job);
      long bytesOutPrev = getOutputBytes(fsStats);
      this.real = job.getOutputFormat().getRecordWriter(fs, job, finalName,
          reporter);
      long bytesOutCurr = getOutputBytes(fsStats);
      fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
    }

    @Override
    public void write(K key, V value) throws IOException {
      long bytesOutPrev = getOutputBytes(fsStats);
      real.write(key, value);
      long bytesOutCurr = getOutputBytes(fsStats);
      fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
      reduceOutputCounter.increment(1);
    }

    @Override
    public void close(Reporter reporter) throws IOException {
      long bytesOutPrev = getOutputBytes(fsStats);
      real.close(reporter);
      long bytesOutCurr = getOutputBytes(fsStats);
      fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
    }

    private long getOutputBytes(List<Statistics> stats) {
      if (stats == null) return 0;
      long bytesWritten = 0;
      for (Statistics stat: stats) {
        bytesWritten = bytesWritten + stat.getBytesWritten();
      }
      return bytesWritten;
    }
  }

  static class NewTrackingRecordWriter<K,V> 
      extends org.apache.hadoop.mapreduce.RecordWriter<K,V> {
    private final org.apache.hadoop.mapreduce.RecordWriter<K,V> real;
    private final org.apache.hadoop.mapreduce.Counter outputRecordCounter;
    private final org.apache.hadoop.mapreduce.Counter fileOutputByteCounter;
    private final List<Statistics> fsStats;

    @SuppressWarnings("unchecked")
    NewTrackingRecordWriter(ReduceTask reduce,
        org.apache.hadoop.mapreduce.TaskAttemptContext taskContext)
        throws InterruptedException, IOException {
      this.outputRecordCounter = reduce.reduceOutputCounter;
      this.fileOutputByteCounter = reduce.fileOutputByteCounter;

      List<Statistics> matchedStats = null;
      if (reduce.outputFormat instanceof org.apache.hadoop.mapreduce.lib.output.FileOutputFormat) {
        matchedStats = getFsStatistics(org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
            .getOutputPath(taskContext), taskContext.getConfiguration());
      }

      fsStats = matchedStats;

      long bytesOutPrev = getOutputBytes(fsStats);
      this.real = (org.apache.hadoop.mapreduce.RecordWriter<K, V>) reduce.outputFormat
          .getRecordWriter(taskContext);
      long bytesOutCurr = getOutputBytes(fsStats);
      fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException,
    InterruptedException {
      long bytesOutPrev = getOutputBytes(fsStats);
      real.close(context);
      long bytesOutCurr = getOutputBytes(fsStats);
      fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
    }

    @Override
    public void write(K key, V value) throws IOException, InterruptedException {
      long bytesOutPrev = getOutputBytes(fsStats);
      real.write(key,value);
      long bytesOutCurr = getOutputBytes(fsStats);
      fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
      outputRecordCounter.increment(1);
    }

    private long getOutputBytes(List<Statistics> stats) {
      if (stats == null) return 0;
      long bytesWritten = 0;
      for (Statistics stat: stats) {
        bytesWritten = bytesWritten + stat.getBytesWritten();
      }
      return bytesWritten;
    }
  }

    private <INKEY,INVALUE,OUTKEY,OUTVALUE>
    void runNewReducer(JobConf job,
                       final TaskUmbilicalProtocol umbilical,
                       final TaskReporter reporter,
                       RawKeyValueIterator rIter,
                       RawComparator<INKEY> comparator,
                       Class<INKEY> keyClass,
                       Class<INVALUE> valueClass,TaskAttemptID newAttemptID,
                       boolean isSort,
                       org.apache.hadoop.mapreduce.TaskAttemptContext taskContext1,
                       org.apache.hadoop.mapreduce.Reducer<INKEY,INVALUE,OUTKEY,OUTVALUE> reducer1,
                       org.apache.hadoop.mapreduce.RecordWriter<OUTKEY,OUTVALUE> trackedRW1,
                       boolean isContention
    ) throws IOException,InterruptedException,
            ClassNotFoundException {
        // wrap value iterator to report progress.
        final RawKeyValueIterator rawIter = rIter;
        rIter = new RawKeyValueIterator() {
            public void close() throws IOException {
                rawIter.close();
            }
            public DataInputBuffer getKey() throws IOException {
                return rawIter.getKey();
            }
            public Progress getProgress() {
                return rawIter.getProgress();
            }
            public DataInputBuffer getValue() throws IOException {
                return rawIter.getValue();
            }
            public boolean next() throws IOException {
                boolean ret = rawIter.next();
                //System.out.printf("rawIter.getProgress().getProgress:\n",rawIter.getProgress().getProgress());
                reporter.setProgress(rawIter.getProgress().getProgress());
                return ret;
            }
        };
        // make a task context so we can get the classes
        org.apache.hadoop.mapreduce.TaskAttemptContext taskContext = isSort?
                new org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl(job,
                        newAttemptID, reporter):taskContext1;

        // make a reducer

        org.apache.hadoop.mapreduce.Reducer<INKEY,INVALUE,OUTKEY,OUTVALUE> reducer = isSort?
                (org.apache.hadoop.mapreduce.Reducer<INKEY,INVALUE,OUTKEY,OUTVALUE>)
                        ReflectionUtils.newInstance(taskContext.getReducerClass(), job):reducer1;

        org.apache.hadoop.mapreduce.RecordWriter<OUTKEY,OUTVALUE> trackedRW = isSort?
                new NewTrackingRecordWriter<OUTKEY, OUTVALUE>(this, taskContext):trackedRW1;


        job.setBoolean("mapred.skip.on", isSkipping());
        job.setBoolean(JobContext.SKIP_RECORDS, isSkipping());

        org.apache.hadoop.mapreduce.Reducer.Context
                reducerContext = createReduceContext(reducer, job, newAttemptID,
                rIter, reduceInputKeyCounter,
                reduceInputValueCounter,
                trackedRW,
                committer,
                reporter, comparator, keyClass,
                valueClass);
        reducer.setReducerID(reduceID);
        reducer.setContention(isContention);
        if(isSort){
          try {
            reducer.setKeyCount(keyCount);
            long rt1 = System.currentTimeMillis();
            reducer.run(reducerContext);
            long rt2 = System.currentTimeMillis();
            LOG.info("gyf.reducer.run.time "+(rt2-rt1));
            keyCount = reducer.getKeyCount();
          } finally {
            long clt1 = System.currentTimeMillis();
            trackedRW.close(reducerContext);
            long clt2 = System.currentTimeMillis();
            LOG.info("gyf.runNewReducer.trackedRW.close "+ (clt2-clt1));
          }
        }
        else{
          reducer.setKeyCount(keyCount);
          long rt1 = System.currentTimeMillis();
          reducer.run(reducerContext);
          long rt2 = System.currentTimeMillis();
          LOG.info("gyf.reducer.run.time "+(rt2-rt1));
          keyCount = reducer.getKeyCount();
        }
    }


    @SuppressWarnings("unchecked")
  private <INKEY,INVALUE,OUTKEY,OUTVALUE>
  void runNewReducer(JobConf job,
                     final TaskUmbilicalProtocol umbilical,
                     final TaskReporter reporter,
                     RawKeyValueIterator rIter,
                     RawComparator<INKEY> comparator,
                     Class<INKEY> keyClass,
                     Class<INVALUE> valueClass,
                     boolean contention
                     ) throws IOException,InterruptedException, 
                              ClassNotFoundException {
    // wrap value iterator to report progress.
    final RawKeyValueIterator rawIter = rIter;
    rIter = new RawKeyValueIterator() {
      public void close() throws IOException {
        rawIter.close();
      }
      public DataInputBuffer getKey() throws IOException {
        return rawIter.getKey();
      }
      public Progress getProgress() {
        return rawIter.getProgress();
      }
      public DataInputBuffer getValue() throws IOException {
        return rawIter.getValue();
      }
      public boolean next() throws IOException {
        boolean ret = rawIter.next();
        //System.out.printf("rawIter.getProgress().getProgress:\n",rawIter.getProgress().getProgress());
        reporter.setProgress(rawIter.getProgress().getProgress());
        return ret;
      }
    };
    // make a task context so we can get the classes
    org.apache.hadoop.mapreduce.TaskAttemptContext taskContext =
      new org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl(job,
          getTaskID(), reporter);
    // make a reducer
    org.apache.hadoop.mapreduce.Reducer<INKEY,INVALUE,OUTKEY,OUTVALUE> reducer =
      (org.apache.hadoop.mapreduce.Reducer<INKEY,INVALUE,OUTKEY,OUTVALUE>)
        ReflectionUtils.newInstance(taskContext.getReducerClass(), job);
    org.apache.hadoop.mapreduce.RecordWriter<OUTKEY,OUTVALUE> trackedRW = 
      new NewTrackingRecordWriter<OUTKEY, OUTVALUE>(this, taskContext);
    job.setBoolean("mapred.skip.on", isSkipping());
    job.setBoolean(JobContext.SKIP_RECORDS, isSkipping());
    org.apache.hadoop.mapreduce.Reducer.Context 
         reducerContext = createReduceContext(reducer, job, getTaskID(),
                                               rIter, reduceInputKeyCounter, 
                                               reduceInputValueCounter, 
                                               trackedRW,
                                               committer,
                                               reporter, comparator, keyClass,
                                               valueClass);
    reducer.setReducerID(reduceID);
      reducer.setContention(contention);
    try {
      reducer.setKeyCount(keyCount);
      reducer.run(reducerContext);
      keyCount = reducer.getKeyCount();
    } finally {
      long t1 = System.currentTimeMillis();
      trackedRW.close(reducerContext);
      long t2 = System.currentTimeMillis();
      LOG.info("gyf.runNewReducer.trackedRW.close: "+ (t2 - t1));
    }
  }

  private <OUTKEY, OUTVALUE>
  void closeQuietly(RecordWriter<OUTKEY, OUTVALUE> c, Reporter r) {
    if (c != null) {
      try {
        c.close(r);
      } catch (Exception e) {
        LOG.info("Exception in closing " + c, e);
      }
    }
  }
}
