val data = sc.textFile("/home/cho/lectures/bdp/mergefile")
data.map(_.split(",") match {
    case Array(latitude, longitude, zero, altitude, date, dates, time) =>
    " 1:" + latitude + " 2:" + longitude
    case _ => "This is nothing"
}).filter(_ != "This is nothing").zipWithIndex.map(x => x._2 + x._1).saveAsTextFile("data")