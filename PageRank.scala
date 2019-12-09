//my pagerank
import org.apache.spark.graphx.GraphLoader

val graph = GraphLoader.edgeListFile(sc, "/home/cho/lectures/bdp/edge.txt")

// Compute the PageRank
val pagerankGraph = graph.pageRank(0.001)
pagerankGraph.vertices.top(1)(Ordering.by(_._2))