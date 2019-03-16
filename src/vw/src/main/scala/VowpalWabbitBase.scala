// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.util.UUID

import org.apache.spark.{ClusterUtil, TaskContext}
import org.apache.spark.ml.{Estimator, Model, PredictionModel, Predictor}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.functions.{col, struct, udf}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row}
import vowpalwabbit.spark.prediction.ScalarPrediction
import vowpalwabbit.spark.{ClusterSpanningTree, VowpalWabbitExample, VowpalWabbitMurmur, VowpalWabbitNative}

import scala.collection.mutable.ArrayBuffer
import scala.math.min
import scala.reflect.ClassTag

case class NamespaceInfo (hash: Int, featureGroup: Char, colIdx: Int)

object VWUtil {
  def autoClose[A <: AutoCloseable,B](closeable: A)(fun: (A) => B): B = {
    try {
      fun(closeable)
    } finally {
      closeable.close()
    }
  }

  def generateNamespaceInfos(featuresCol:String, additionalFeatures:Seq[String], hashSeed:Int, schema:StructType):Seq[NamespaceInfo] =
   (Seq(featuresCol) ++ additionalFeatures)
    .map(col => new NamespaceInfo(
      VowpalWabbitMurmur.hash(col, hashSeed),
      col.charAt(0),
      schema.fieldIndex(col)))
}

@InternalWrapper
trait VowpalWabbitBase extends DefaultParamsWritable
  with org.apache.spark.ml.param.shared.HasWeightCol
  with org.apache.spark.ml.param.shared.HasFeaturesCol
  with org.apache.spark.ml.param.shared.HasLabelCol
{
  // can we switch to https://docs.scala-lang.org/overviews/macros/paradise.html ?
  val args = new Param[String](this, "args", "VW command line arguments passed")
  setDefault(args -> "")

  def getArgs: String = $(args)
  def setArgs(value: String): this.type = set(args, value)

  val additionalFeatures = new StringArrayParam(this, "additionalFeatures", "Additional feature columns")
  setDefault(additionalFeatures -> Array())

  def getAdditionalFeatures: Array[String] = $(additionalFeatures)
  def setAdditionalFeatures(value: Array[String]): this.type = set(additionalFeatures, value)

  // to support Grid search we need to replicate the parameters here...
  val numPasses = new IntParam(this, "numPasses", "Number of passes over the data")
  setDefault(numPasses -> 1)
  def getNumPasses: Int = $(numPasses)
  def setNumPasses(value: Int): this.type = set(numPasses, value)

  val learningRate = new DoubleParam(this, "learningRate", "Learning rate")
  def getLearningRate: Double = $(learningRate)
  def setLearningRate(value: Double): this.type = set(learningRate, value)

  val powerT = new DoubleParam(this, "powerT", "t power value")
  def getPowerT: Double = $(powerT)
  def setPowerT(value: Double): this.type = set(powerT, value)

  val l1 = new DoubleParam(this, "l1", "l_1 lambda")
  def getL1: Double = $(l1)
  def setL1(value: Double): this.type = set(l1, value)

  val l2 = new DoubleParam(this, "l2", "l_2 lambda")
  def getL2: Double = $(l2)
  def setL2(value: Double): this.type = set(l2, value)

  val interactions = new StringArrayParam(this, "interactions", "Interaction terms as specified by -q")
  def getInteractions: Array[String] = $(interactions)
  def setInteractions(value: Array[String]): this.type = set(interactions, value)

  val ignoreNamespaces = new Param[String](this, "ignoreNamespace", "Namespaces to be ignored (first letter only)")
  def getIgnoreNamespaces: String = $(ignoreNamespaces)
  def setIgnoreNamespaces(value: String): this.type = set(ignoreNamespaces, value)

  implicit class ParamStringBuilder(sb: StringBuilder) {
    def appendParam[T](param: Param[T], option: String): StringBuilder = {
      if (!get(param).isEmpty) {

        param match {
          case _: StringArrayParam => {
            for (q <- get(param).get)
              sb.append(option).append(' ').append(q)
          }
          case _ => sb.append(option).append(' ').append(get(param).get)
        }
      }

      sb
    }
  }

  val hashSeed = new IntParam(this, "hashSeed", "Seed used for hashing")
  setDefault(hashSeed -> 0)

  def getHashSeed: Int = $(hashSeed)
  def setHashSeed(value: Int): this.type = set(hashSeed, value)

  val numBits = new IntParam(this, "numbits", "Number of bits used")
  setDefault(numBits -> 18)

  def getNumBits: Int = $(numBits)
  def setNumBits(value: Int): this.type = set(numBits, value)


  private def trainInternal(df: DataFrame, vwArgs: String, contextArgs: () => String = () => "") = {
    val labelColIdx = df.schema.fieldIndex(getLabelCol)

    val applyLabel = if (get(weightCol).isDefined) {
      val weightColIdx = df.schema.fieldIndex(getWeightCol)
      (row:Row, ex:VowpalWabbitExample) => ex.setLabel(row.getDouble(weightColIdx).toFloat, row.getDouble(labelColIdx).toFloat)
    }
    else
      (row:Row, ex:VowpalWabbitExample) => ex.setLabel(row.getDouble(labelColIdx).toFloat)

    println(vwArgs) // TODO: properly log

    val featureColIndices = VWUtil.generateNamespaceInfos(getFeaturesCol, getAdditionalFeatures, getHashSeed, df.schema)

    def trainIteration(inputRows: Iterator[Row]): Iterator[Array[Byte]] = {
      val numPasses = getNumPasses

      val iteratorGenerator = if (numPasses == 1)
      // avoid caching inputRows
        () => inputRows
      else {
        // cache all row references to be able to quickly get the iterator again
        val inputRowsArr = inputRows.toArray
        () => inputRowsArr.iterator
      }

      val args = s"$vwArgs ${contextArgs()}"

      VWUtil.autoClose(new VowpalWabbitNative(args)) { vw =>
        VWUtil.autoClose(vw.createExample()) { ex =>
          // loop in here to avoid
          // - passing model back and forth
          // - re-establishing network connection
          for (_ <- 0 to getNumPasses) {
            // pass data to VW native part
            val it = iteratorGenerator()
            while (it.hasNext) {
              val row = it.next

              // transfer label
              applyLabel(row, ex)

              // transfer features
              for (ns <- featureColIndices)
                row.get(ns.colIdx) match {
                  case dense: DenseVector => ex.addToNamespaceDense(ns.featureGroup,
                    ns.hash, dense.values)
                  case sparse: SparseVector => ex.addToNamespaceSparse(ns.featureGroup,
                    sparse.indices, sparse.values)
                }

              ex.learn
              ex.clear
            }

            vw.endPass
          }
        }

        Seq(vw.getModel).toIterator
      }
    }

    val encoder = Encoders.kryo[Array[Byte]]
    df.mapPartitions(trainIteration)(encoder).reduce((m1, _) => m1)
  }

  protected def trainInternal(dataset: Dataset[_]): Array[Byte] = {
    // follow LightGBM pattern
    val numCoresPerExec = ClusterUtil.getNumCoresPerExecutor(dataset)
    val numExecutorCores = ClusterUtil.getNumExecutorCores(dataset, numCoresPerExec)
    val numWorkers = min(numExecutorCores, dataset.rdd.getNumPartitions)

    // Reduce number of partitions to number of executor cores
    val df = dataset.toDF().coalesce(numWorkers).cache()

    val numPartitions = df.rdd.getNumPartitions

    val vwArgs = new StringBuilder()
      .append(s"${getArgs} --save_resume --preserve_performance_counters ")
      .append(s"--hash_seed ${getHashSeed} ").append(s"-b ${getNumBits} ")
      .appendParam(learningRate, "-l").appendParam(powerT, "--power_t")
      .appendParam(l1, "--l1").appendParam(l2, "--l2")
      .appendParam(ignoreNamespaces, "--ignore").appendParam(interactions, "-q")

    val binaryModel = if (numPartitions == 1)
      trainInternal(df, vwArgs.result)
    else  {
      // multiple partitions -> setup distributed coordination
      val spanningTree = new ClusterSpanningTree(0)

      try {
        spanningTree.start

        val jobUniqueId = Math.abs(UUID.randomUUID.getLeastSignificantBits.toInt)
        val driverHostAddress = ClusterUtil.getDriverHost(dataset)
        val port = spanningTree.getPort

        /*
        --span_server specifies the network address of a little server that sets up spanning trees over the nodes.
        --unique_id should be a number that is the same for all nodes executing a particular job and
          different for all others.
        --total is the total number of nodes.
        --node should be unique for each node and range from {0,total-1}.
        --holdout_off should be included for distributed training
        */
        vwArgs.append(s" --holdout_off --span_server $driverHostAddress --span_server_port $port --unique_id $jobUniqueId --total $numPartitions ")

        trainInternal(df, vwArgs.result, () => s"--node ${TaskContext.get.partitionId}")
      } finally {
        spanningTree.stop
      }
    }

    binaryModel
  }
}

trait VowpalWabbitBaseModel extends org.apache.spark.ml.param.shared.HasFeaturesCol with org.apache.spark.ml.param.shared.HasRawPredictionCol
{
  val model: Array[Byte]

  @transient
  lazy val vw = new VowpalWabbitNative("--testonly --quiet", model)

  @transient
  lazy val example = vw.createExample()

  @transient
  lazy val vwArgs = vw.getArguments


  val additionalFeatures = new StringArrayParam(this, "additionalFeatures", "Additional feature columns")
  def getAdditionalFeatures: Array[String] = $(additionalFeatures)
  def setAdditionalFeatures(value: Array[String]): this.type = set(additionalFeatures, value)

  protected def transformImplInternal(dataset: Dataset[_]): DataFrame = {
    val featureColIndices = (Seq(getFeaturesCol) ++ getAdditionalFeatures)
      .zipWithIndex.map { case (col, index) => new NamespaceInfo(
        VowpalWabbitMurmur.hash(col, vwArgs.getHashSeed),
        col.charAt(0),
        index)
    }

    val predictUDF = udf { (namespaces: Row) => predictInternal(featureColIndices, namespaces) }

    val allCols = Seq(col($(featuresCol))) ++ getAdditionalFeatures.map(col)

    dataset.withColumn($(rawPredictionCol), predictUDF(struct(allCols: _*)))
  }

  protected def predictInternal(featureColIndices:Seq[NamespaceInfo], namespaces: Row): Double = {
    example.clear

    for (ns <- featureColIndices)
      namespaces.get(ns.colIdx) match {
        case dense: DenseVector => example.addToNamespaceDense(ns.featureGroup,
          ns.hash, dense.values)
        case sparse: SparseVector => example.addToNamespaceSparse(ns.featureGroup,
          sparse.indices, sparse.values)
      }

    // TODO: surface confidence
    example.predict.asInstanceOf[ScalarPrediction].getValue.toDouble
  }
}