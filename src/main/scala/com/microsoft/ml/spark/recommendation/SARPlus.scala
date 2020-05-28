package com.microsoft.ml.spark.recommendation

import com.microsoft.ml.spark.core.contracts.Wrappable
import com.microsoft.ml.spark.core.env.InternalWrapper
import org.apache.spark.ml.param.{DataFrameParam, Param, ParamMap}
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Estimator, Model}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types.{ArrayType, DoubleType, FloatType, IntegerType, LongType, StructField, StructType}
import org.apache.spark.sql.functions.{col, collect_list, count, exp, max, row_number, sum, udf, window}
import org.apache.spark.storage.StorageLevel
import java.util.Arrays

import org.apache.spark.ml.recommendation.Constants
import org.apache.spark.sql.catalyst.encoders.RowEncoder

import scala.collection.mutable

@InternalWrapper
class SARPlus(override val uid: String) extends Estimator[SARPlusModel]
  with SARParams with DefaultParamsWritable {

  private def applyTimeDecay(df: DataFrame) =
    if ($(timeDecayEnabled)) {
      val latestTimestamp = df.select(max(col($(timeCol)).cast(LongType))).collect()(0).getLong(0)

      // apply time decay formula
      df.groupBy($(userCol), $(itemCol))
        .agg(sum(col($(ratingCol)) *
          exp((-col($(timeCol)).cast(LongType) + latestTimestamp) * -Math.log(2.0) /
            ($(timeDecayCoeff) * 3600 * 24))).alias($(ratingCol)))
        .repartition(col($(userCol)))
        .sortWithinPartitions()
    }
    else if (df.schema.fieldNames.contains($(timeCol)))
      df
        // TODO: safe way to generate column name
        .withColumn("MY_IDX", row_number.over(Window.partitionBy($(userCol), $(itemCol)).orderBy(col($(timeCol)).desc)))
        .where(col("MY_IDX") === 1)
        .select($(userCol), $(itemCol), $(ratingCol))
    else
      df

  private def computeCoocurrence(df: DataFrame) = {
    val dfLeft = df.select(
      col($(userCol)).alias("u1"),
      col($(itemCol)).alias("i1")
    )

    val dfRight = df.select(
      col($(userCol)).alias("u2"),
      col($(itemCol)).alias("i2")
    )

    // compute co-occurrence above minimum threshold
    dfLeft
      .crossJoin(dfRight)
      .where(
        (col("u1") === col("u2")) &&
        (col("i1") <= col("i2")))
      .groupBy("i1", "i2")
      .agg(count("*").alias("value"))
      .where(col("value") >= $(supportThreshold))
      .repartition(col("i1"), col("i2"))
      .sortWithinPartitions()
  }

  private def computeItemSimilarity(itemCoocurrence: DataFrame) = {
    val itemMarginal =
      itemCoocurrence
        .where(col("i1") === col("i2"))
        .select(
          col("i1").alias("i"),
          col("value").alias("margin"))

    val itemMarginal1 = itemMarginal.select(
      col("i").alias("iref1"),
      col("margin").alias("margin1")
    )

    val itemMarginal2 = itemMarginal.select(
      col("i").alias("iref2"),
      col("margin").alias("margin2")
    )

    val itemCoocurrenceWithMarginals =
      itemCoocurrence
        .join(itemMarginal1, col("i1") === col("iref1"))
        .join(itemMarginal2, col("i2") === col("iref2"))

    $(similarityFunction) match {
      case "jaccard" =>
        itemCoocurrenceWithMarginals.select(
          col("i1"),
          col("i2"),
          (col("value") /
            (col("margin1") + col("margin2") - col("value")))
            .alias("value").cast(FloatType)
        )
      case "lift" =>
        itemCoocurrenceWithMarginals.select(
          col("i1"),
          col("i2"),
          (col("value") /
            (col("margin1") * col("margin2")))
            .alias("value").cast(FloatType)
        )
      case _ => itemCoocurrence.select(
        col("i1"),
        col("i2"),
        col("value").cast(FloatType)
      )
    }
  }

  private def expandUpperTriangular(itemSimilarityUpper: DataFrame) =
    itemSimilarityUpper.union(
      itemSimilarityUpper
        .where(col("i1") =!= col("i2"))
        .select(
          col("i2").alias("i1"),
          col("i1").alias("i2"),
          col("value"))
    )
    .repartition(col("i1"), col("i2"))
    .sortWithinPartitions()

  private def computeItemMapping(itemSimilarity: DataFrame) =
    // create item id to continuous index mapping
    itemSimilarity
      .select("i1")
      .distinct()
      .withColumn("idx", row_number().over(Window.orderBy("i1")).cast(IntegerType) - 1)
      .repartition(col("i1"))
      .sortWithinPartitions()

  private def computeFastLookupIndex(itemSimilarity: DataFrame, itemMapping: DataFrame, it: Iterator[Row]) = {
    val itemCount = itemMapping.count
    val similarityCount = itemSimilarity.count

    // construct the lookup arrays
    val related = Array.ofDim[Int](itemCount.toInt + 1)
    val similarityItem2 = Array.ofDim[Int](similarityCount.toInt)
    val similarityValue = Array.ofDim[Float](similarityCount.toInt)

    var lastId = Int.MaxValue
    var idxRelated = 0
    var rowNumber = 0

    for (row <- it) {
      val i1 = row.getInt(0)
      val i2 = row.getInt(1)
      val value = row.getFloat(2)

      if (lastId != i1)
      {
        related(idxRelated) = rowNumber
        idxRelated += 1
        lastId = i1
      }

      similarityItem2(rowNumber) = i2
      similarityValue(rowNumber) = value

      rowNumber += 1
    }

    // write final one to ease range lookup
    related(related.length - 1) = rowNumber

//    Seq(SARModelInternal(related, similarityItem2, similarityValue)).iterator
    SARModelInternal(related, similarityItem2, similarityValue)
  }

  private def computeFastLookup(itemSimilarity: DataFrame, itemMapping: DataFrame) = {
    val itemMapping1 = itemMapping.select(
      col("i1").alias("iref1"),
      col("idx").alias("idx1")
    )
    val itemMapping2 = itemMapping.select(
      col("i1").alias("iref2"),
      col("idx").alias("idx2")
    )

    val itemSimilarityCollected =
    itemSimilarity
      .join(itemMapping1, col("i1") === col("iref1"))
      .join(itemMapping2, col("i2") === col("iref2"))
      .select(
        col("idx1").alias("i1"),
        col("idx2").alias("i2"),
        col("value"))
      // TODO: with rangePartitions &  treeAggregate it should be possible to scale further
//      .repartitionByRange(col("i1"))
//      .sortWithinPartitions(col("i1"), col("i2"))
      .orderBy(col("i1"), col("i2"))
      .collect()

    computeFastLookupIndex(itemSimilarity, itemMapping, itemSimilarityCollected.iterator)
  }

  def this() = this(Identifiable.randomUID("SARPlus"))

  override def fit(dataset: Dataset[_]): SARPlusModel = {

    val df = applyTimeDecay(dataset.toDF())

    val itemCoocurrence = computeCoocurrence(df)
      .persist(StorageLevel.DISK_ONLY_2)

    val itemSimilarityUpper = computeItemSimilarity(itemCoocurrence)
      .persist(StorageLevel.DISK_ONLY_2)
    itemCoocurrence.unpersist()

    val itemSimilarity = expandUpperTriangular(itemSimilarityUpper)
      .persist(StorageLevel.DISK_ONLY_2)
    itemSimilarityUpper.unpersist()

    val itemMapping = computeItemMapping(itemSimilarity)
      .cache()

    val index = computeFastLookup(itemSimilarity, itemMapping)
    itemSimilarity.unpersist()

    new SARPlusModel(uid)
      .setUserDataFrame(df)
      .setItemMapping(itemMapping)
      .setIndex(index)
      .setItemCol($(itemCol))
      .setUserCol($(userCol))
  }

  override def copy(extra: ParamMap): Estimator[SARPlusModel] = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType =
    validateAndTransformSchema(schema)
}

case class SARModelInternal(related: Array[Int], similarityItem2: Array[Int], similarityValue: Array[Float])

object SARPlus extends DefaultParamsReadable[SARPlus]

@InternalWrapper
class SARPlusModel(override val uid: String) extends Model[SARPlusModel]
  with SARParams with Wrappable with ComplexParamsWritable {

  def setUserDataFrame(value: DataFrame): this.type = set(userDataFrame, value)
  val userDataFrame = new DataFrameParam(this, "userDataFrame", "User activity")

  val itemMapping = new DataFrameParam(this, "itemMapping", "Mapping items to indexed space")
  def setItemMapping(value: DataFrame): this.type = set(itemMapping, value)

  val index = new Param[SARModelInternal](this, "index", "Internal lookup structure")
  def setIndex(value: SARModelInternal): this.type = set(index, value)

  def this() = this(Identifiable.randomUID("SARPlusModel"))

  override def copy(extra: ParamMap): SARPlusModel =
    new SARPlusModel(uid)
      .setItemMapping($(itemMapping))
      .setIndex($(index))

  def recommendForAllUsers(numItems: Int): DataFrame =
    recommendForUserSubset($(userDataFrame), numItems)

  def recommendForUserSubset(users: DataFrame, numItems: Int): DataFrame = {
//    val zipper = udf[Seq[(Double, Float)], Seq[Double], Seq[Float]](_.zip(_))
    val zipper = udf((items: Seq[Double], scores: Seq[Float]) => {
      // TODO: WHY does the sort order change the metric results?
      items.zip(scores)
        .sortWith(_._1 < _._1)
      // TODO: don't get the implication of sort here
//        .sortWith((x, y) => x._2 > y._2 || (x._2 == y._2 && x._1 < y._2))
    })

    val recommendationArrayType =
      ArrayType(
        StructType(Array(StructField(getItemCol, IntegerType),
          StructField(getPredictionCol, FloatType))))

    recommend(users, false, numItems)
      .groupBy($(userCol))
      // group items and scores into 2 arrays
      .agg(
        collect_list(col($(itemCol))).alias("items"),
        collect_list(col("score")).alias("scores"))
      // zip items and scores array up
//      .selectExpr(
//        $(userCol),
//        s"array_zip(items, scores) AS ${Constants.Recommendations}")
      // cast the array into the right type
      .select(
        col($(userCol)),
//        col(Constants.Recommendations).cast(recommendationArrayType)
        zipper(col("items"), col("scores"))
          .alias(Constants.Recommendations).cast(recommendationArrayType)
          )
  }

  def recommend(dataset: DataFrame, removeSeen: Boolean, topK: Int): DataFrame = {
    val broadcastIndex = dataset.sparkSession.sparkContext.broadcast($(index))

    val testDf = dataset.toDF

    val encoder = RowEncoder(StructType(
      dataset.schema.apply($(userCol)) ::
      StructField("idxOutput", IntegerType, true) ::
      StructField("score", FloatType, true) :: Nil))

    val userList = testDf
      .select(col($(userCol)).as("USER_INPUT_COL")) // TODO: unique column name
      .distinct()
      .repartition(col("USER_INPUT_COL"))

    val topItems = userList
      .join($(userDataFrame), $(userDataFrame).col($(userCol)) === col("USER_INPUT_COL"))
      .join($(itemMapping), col($(itemCol)) === $(itemMapping).col("i1"))
      .select(
        $(userDataFrame).col($(userCol)),
        $(itemMapping).col("idx"),
        $(userDataFrame).col($(ratingCol)).cast(DoubleType))
      .repartition(col($(userCol)))
      .sortWithinPartitions(col("idx"))
      .mapPartitions(it => {
        new ProcessingIterator(it, broadcastIndex.value, removeSeen, topK)
     })(encoder)

    topItems
      .join($(itemMapping), col("idxOutput") === $(itemMapping).col("idx"))
      .select(
        col($(userCol)),
        col("i1").alias($(itemCol)),
        col("score")
      )

    // x.orderBy($(userCol), "i1", "score").show(30)

//    x
  }

  class ProcessingIterator(it: Iterator[Row], val fastIndex: SARModelInternal, val removeSeen: Boolean, val topK: Int)
    extends Iterator[Row] {
    var lastRow = Option.empty[Row]
    val topKItems = mutable.PriorityQueue[ItemScore]()(Ordering.by(is => -is.score))

    override def hasNext: Boolean = !topKItems.isEmpty || it.hasNext || !lastRow.isEmpty

    override def next(): Row = {
      if (topKItems.isEmpty) {
        // get all user items & ratings for 1 user
        val (userId, userItems, userRatings) = collectUserItems

        // initial seen items
        val seenItems = mutable.HashSet[Int]()
        if (removeSeen)
          seenItems ++= userItems

        // loop through items user has seen
        for (i1 <- userItems.indices) {
          val relatedBeg = fastIndex.related(i1)
          val relatedEnd = fastIndex.related(i1 + 1)

          for (relatedOffset <- relatedBeg until relatedEnd) {
            val relatedItem = fastIndex.similarityItem2(relatedOffset)

            if (!seenItems.contains(relatedItem)) {
              // avoid duplicates
              seenItems += relatedItem

              // calculate score
              val relatedItemScore = joinProdSum(fastIndex, userItems, userRatings, relatedItem)

              if (relatedItemScore > 0) {
                if (topKItems.size < topK)
                  topKItems += ItemScore(userId, relatedItem, relatedItemScore)
                else {
                  if (topKItems.head.score < relatedItemScore) {
                    topKItems.dequeue
                    topKItems += ItemScore(userId, relatedItem, relatedItemScore)
                  }
                }
              }
            }
          }
        }

        // index is 0 to n. thus -1 won't match in the join when mapping back
        if (topKItems.isEmpty)
          topKItems += ItemScore(userId, -1, 0f)
      }

      val itemScore = topKItems.dequeue
      Row.fromTuple((itemScore.userId, itemScore.idx, itemScore.score))
    }

    private def collectUserItems() = {
      val userItemsBuilder = mutable.ArrayBuilder.make[Int]
      val userRatingsBuilder = mutable.ArrayBuilder.make[Double]

      // continue from last row if present
      var row = if (lastRow.isEmpty)
        it.next
      else {
        val v = lastRow.get
        lastRow = None
        v
      }

      val currentUserId = row.get(0)
      var loopCondition = true
      while (loopCondition) {
        userItemsBuilder += row.getInt(1)
        userRatingsBuilder += row.getDouble(2)

        if (it.hasNext) {
          row = it.next
          val userId = row.get(0)
          if (userId != currentUserId) {
            loopCondition = false
            lastRow = Some(row)
          }
        }
        else
          loopCondition = false
      }

      (currentUserId, userItemsBuilder.result, userRatingsBuilder.result)
    }

    /**
      * Joins the userItems with the corresponding item array in fastIndex.
      * As both arrays are sorted we advance in parallel as we find matches.
      */
    private def joinProdSum(fastIndex: SARModelInternal,
                            userItems: Array[Int],
                            userRatings: Array[Double],
                            relatedId: Int): Float = {
      var contribPtr = fastIndex.related(relatedId)
      val contribEnd = fastIndex.related(relatedId + 1)

      var userPtr = 0
      var score = 0f

      var userItemId = userItems(userPtr)
      var contribItemId = fastIndex.similarityItem2(contribPtr)

      while (true) {
        if (userItemId < contribItemId) {
          // userItemId is smaller, let's find check if the contribItemId is in here
          // and advance the userPtr
          userPtr = Arrays.binarySearch(userItems, userPtr, userItems.length, contribItemId)
          if (userPtr < 0)
            return score

          userItemId = userItems(userPtr)
        }
        else if (userItemId > contribItemId) {
          contribPtr = Arrays.binarySearch(fastIndex.similarityItem2, contribPtr, contribEnd, userItemId)
          if (contribPtr < 0)
            return score

          contribItemId = fastIndex.similarityItem2(contribPtr)
        }
        else {
          score += userRatings(userPtr).toFloat * fastIndex.similarityValue(contribPtr)

          userPtr += 1
          if (userPtr == userItems.length)
            return score

          contribPtr += 1
          if (contribPtr == contribEnd)
            return score

          userItemId = userItems(userPtr)
          contribItemId = fastIndex.similarityItem2(contribPtr)
        }
      }

      score
    }
  }

  case class ItemScore(userId: Any, idx: Int, score: Float)

  override def transform(dataset: Dataset[_]): DataFrame =
    recommend(dataset.toDF, false, 5)

  override def transformSchema(schema: StructType): StructType =
    schema
}

object SARPlusModel extends ComplexParamsReadable[SARPlusModel]
