package com.microsoft.ml.spark.recommendation

import com.microsoft.ml.spark.core.test.fuzzing.{EstimatorFuzzing, TestObject, TransformerFuzzing}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame

class SARPlusSpec extends RankingTestBase with EstimatorFuzzing[SARPlus] {
  override def testObjects(): Seq[TestObject[SARPlus]] =
    List(
      new TestObject(new SARPlus()
        .setUserCol(recommendationIndexer.getUserOutputCol)
        .setItemCol(recommendationIndexer.getItemOutputCol)
        .setRatingCol(ratingCol), transformedDf)
    )

  override def reader: SARPlus.type = SARPlus

  override def modelReader: SARPlusModel.type = SARPlusModel

  test("SARPLusMovieLens") {
    val sarplus = new SARPlus()
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
      .setTimeCol("timestamp")
      .setSupportThreshold(3)

    val df = session.read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("/mnt/c/Data/MovieLens25m/ratings1m.csv")

    df.show(10)

    var t0 = System.nanoTime

    val model = sarplus.fit(df)

    println(s"TIME: ${java.time.Duration.of(System.nanoTime - t0, java.time.temporal.ChronoUnit.NANOS)}")
    t0 = System.nanoTime

    val recommendation = model.recommend(df, false, 5)

    recommendation.show(10)
    println(s"TIME: ${java.time.Duration.of(System.nanoTime - t0, java.time.temporal.ChronoUnit.NANOS)}")
  }

  test("SARPlus") {

    val algo = sarplus
      .setTimeDecayEnabled(false)
      .setSupportThreshold(1)
      .setSimilarityFunction("jacccard")


    val adapter: RankingAdapter = new RankingAdapter()
      .setK(5)
      .setRecommender(algo)

    val recopipeline = new Pipeline()
      .setStages(Array(recommendationIndexer, adapter))
      .fit(ratings)

    val output = recopipeline.transform(ratings)

    val evaluator: RankingEvaluator = new RankingEvaluator()
      .setK(5)
      .setNItems(10)

    assert(evaluator.setMetricName("ndcgAt").evaluate(output) === 0.7168)
    assert(evaluator.setMetricName("fcp").evaluate(output) === 0.05)
    assert(evaluator.setMetricName("mrr").evaluate(output) === 1.0)

    val users: DataFrame = session
      .createDataFrame(Seq(("0","0"),("1","1")))
      .toDF(userColIndex, itemColIndex)

    val recs = recopipeline.stages(1).asInstanceOf[RankingAdapterModel].getRecommenderModel
      .asInstanceOf[SARPlusModel].recommendForUserSubset(users, 10)
    assert(recs.count == 2)
  }
}

class SARPlusModelSpec extends RankingTestBase with TransformerFuzzing[SARPlusModel] {
  override def testObjects(): Seq[TestObject[SARPlusModel]] = {
    List(
      new TestObject(new SARPlus()
        .setUserCol(recommendationIndexer.getUserOutputCol)
        .setItemCol(recommendationIndexer.getItemOutputCol)
        .setRatingCol(ratingCol)
        .fit(transformedDf), transformedDf)
    )
  }

  override def reader: MLReadable[_] = SARPlusModel
}