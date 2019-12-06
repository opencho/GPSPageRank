import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator

val data = sc.textFile("/home/cho/lectures/bdp/20081023025304.plt")

class Position(latitude: Double, longitude: Double) extends Serializable {
    def x = latitude
    def y = longitude
}

val positions = data.zipWithIndex.filter(x => x._2 > 5).map(x => x._1).map(_.split(",") match {
    case Array(latitude, longitude, zero, altitude, date, dates, time) =>
        new Position(latitude.toDouble, longitude.toDouble)
})

val positions = data.zipWithIndex.filter(x => x._2 > 5).map(x => (x._2-6, x._1)).map(x => x._2.split(",") match {
    case Array(latitude, longitude, zero, altitude, date, dates, time) =>
        (x._1.toDouble, IndexedSeq(latitude.toDouble, longitude.toDouble))
})

val dataset = positions.toDF()

class Trajectory(src: Position, dest: Position) extends Serializable {
    def source = src
    def destination = dest
}

val dataset = spark.read.format("libsvm").load("/home/cho/local/spark-2.4.4/data/mllib/sample_kmeans_data.txt")

// Trains a k-means model.
val kmeans = new KMeans().setK(2).setSeed(1L)
val model = kmeans.fit(dataset)

// Make predictions
val predictions = model.transform(dataset)

// Evaluate clustering by computing Silhouette score
val evaluator = new ClusteringEvaluator()

val silhouette = evaluator.evaluate(predictions)
println(s"Silhouette with squared euclidean distance = $silhouette")