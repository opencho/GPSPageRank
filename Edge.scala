import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.sql.DataFrame


// make Data Frame
import org.apache.spark.ml.linalg.{Vector,Vectors}
def makeDF(index: Int,latitude: Double, longitude: Double) = {
    def toFeatures = Vectors.sparse(2,Array(0,1),Array(latitude,longitude))
    List((index,toFeatures)).toDF("label","features")
}
def toFeatures(latitude: Double, longitude: Double) = {
    Vectors.sparse(2,Array(0,1),Array(latitude,longitude))
}
//load
val model = KMeansModel.load("/home/cho/lectures/bdp/kmeansModel")
val data = sc.textFile("/home/cho/lectures/bdp/mergefile2")
val dataset= data.zipWithIndex.map(x => x._1.split(",") match {
    case Array(latitude, longitude, zero, altitude, date, dates, time,id) => (x._2.toDouble, toFeatures(latitude.toDouble,longitude.toDouble), date.toDouble,dates,id.toInt)
    case _ => (-1.0,toFeatures(0.0,0.0),0.0,"",0)
}).filter(_._1 > -1.0).toDF("label","features","time","date","id")

val predictions = model.transform(dataset)
predictions.orderBy("time").select("id","prediction").repartition(1).write.save("prediction")

