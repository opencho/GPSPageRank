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
predictions.select("time","id","prediction").dropDuplicates("id","prediction").orderBy("time").select("id","prediction").repartition(1).write.csv("prediction.csv")

//just scala not spark
import scala.io.Source
import java.io.File
import java.io.PrintWriter
def makeEdge(data: List[String]): List[(String,String)] = {
    data zip data.tail
}
val writer = new PrintWriter(new File("/home/cho/lectures/bdp/edge.txt"))
var id = -1
val data = Source.fromFile("/home/cho/lectures/bdp/tmp.csv").getLines().map(_.split(",") match { case Array(id,node) => (id, node)}).toList
data.groupBy(_._1).map(_._2.map(_._2)).toList.map(makeEdge)
data.groupBy(_._1).map(_._2.map(_._2)).toList.map(makeEdge).filter(_ != Nil).map(_.map(x => x._1 + " " + x._2)).map(_.mkString("\n") + "\n").mkString

writer.write(data.groupBy(_._1).map(_._2.map(_._2)).toList.map(makeEdge).filter(_ != Nil).map(_.map(x => x._1 + " " + x._2)).map(_.mkString("\n") + "\n").mkString)
writer.close()   