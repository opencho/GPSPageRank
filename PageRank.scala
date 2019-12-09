//my pagerank
import org.apache.spark.graphx.GraphLoader

val graph = GraphLoader.edgeListFile(sc, "/home/cho/lectures/bdp/edge.txt")

// Compute the PageRank
val pagerankGraph = graph.pageRank(0.001)
pagerankGraph.vertices.top(1)(Ordering.by(_._2))

//Load the k-means model
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.clustering.KMeansModel

val model = KMeansModel.load("/home/cho/lectures/bdp/kmeansModel")

// Transform a GPS to a Graph node
import org.apache.spark.ml.linalg.{Vector,Vectors}
def makeDF(index: Int,latitude: Double, longitude: Double) = {
    def toFeatures = Vectors.sparse(2,Array(0,1),Array(latitude,longitude))
    List((index,toFeatures)).toDF("label","features")
}

val latitude = 39.0
val longitude = 116.0

val currentNode = model.transform(makeDF(0,latitude,longitude)).select("prediction").first.mkString.toInt

// Recommend high rank nodes
val candidates = pagerankGraph.subgraph(triplet => triplet.srcId == currentNode).edges.map(_.dstId.toInt).collect
pagerankGraph.vertices.filter(x => candidates.contains(x._1)).top(5)(Ordering.by(_._2))