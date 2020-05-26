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

    assert(evaluator.setMetricName("ndcgAt").evaluate(output) == 0.7168486344464263)
    assert(evaluator.setMetricName("fcp").evaluate(output) == 0.05000000000000001)
    assert(evaluator.setMetricName("mrr").evaluate(output) == 1.0)

    val users: DataFrame = session
      .createDataFrame(Seq(("0","0"),("1","1")))
      .toDF(userColIndex, itemColIndex)

    val recs = recopipeline.stages(1).asInstanceOf[RankingAdapterModel].getRecommenderModel
      .asInstanceOf[SARModel].recommendForUserSubset(users, 10)
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