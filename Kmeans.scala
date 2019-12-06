import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.ml.evaluation.ClusteringEvaluator

// Loads data.
val dataset = spark.read.format("libsvm").load("/home/cho/lectures/bdp/data.txt")

// Trains a k-means model.
val kmeans = new KMeans().setK(3000).setSeed(1L)
val model = kmeans.fit(dataset)

// Make predictions
val predictions = model.transform(dataset)

// Evaluate clustering by computing Silhouette score
// val evaluator = new ClusteringEvaluator()

// val silhouette = evaluator.evaluate(predictions)
// println(s"Silhouette with squared euclidean distance = $silhouette")

// Shows the result.
println("Cluster Centers: ")
model.clusterCenters.foreach(println)
sc.parallelize(model.clusterCenters.map(_.toString.filter(x => x != '[' && x != ']'))).saveAsTextFile("centroids30")

//load
val sameModel = KMeansModel.load("/home/cho/lectures/bdp/kmeansModel")

sc.textFile("/home/cho/lectures/bdp/mergefile").map(_.split(",") match {
    case Array(latitude, longitude, zero, altitude, date, dates, time,index) => 
        Array(latitude, longitude, zero, altitude, date, dates, time, index).mkString(",")
    case _ => "This is nothing"
}).filter(_ != "This is nothing").saveAsTextFile("mergefile2")
