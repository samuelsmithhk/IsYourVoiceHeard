val roll_call = sc.textFile("/user/yl3935/project/votes.csv")
/* data looks like Array[String] = Array(Alexander (R-TN),282,Yea)*/
val roll_call_rdd = roll_call.map(line => line.split(",")).map(line => (line(0),(line(1),line(2))))
/*data now looks like  Array[(String, (String, String))] = Array((Alexander (R-TN),(282,Yea)))*/
val name = sc.textFile("/user/yl3935/project/name.csv")
/*data looks like Array[String] = Array("Alexander, Lamar"	TN	R)*/
val name_rdd = name.map(line => line.split("\""))
				.map(line => (line(1).split(",")(0),line(1),line(2).slice(1,3),line(2).slice(4,5)))
				.map(line => (Array(line._1," ", "(",line._4,"-",line._3,")").mkString(""),line._2,line._3,line._4))
				.map(line => (line._1,(line._2,line._3,line._4)))
/*now data looks like Array[(String, (String, String, String))] = Array((Alexander (R-TN),(Alexander, Lamar,TN,R)))*/
val full_name = (roll_call_rdd join name_pair).map(line => line._2).map(line => (line._2._1, line._2._2,line._2._3,line._1._1,line._1._2))

full_name.saveAsTextFile("project/full_name_roll_call")

val summary = sc.textFile("/user/yl3935/project/summary.csv").map(line => line.split(","))
			.map(line => (line(0),line(1),line(2),line.slice(3,7).mkString("")))

summary.saveAsTextFile("project/summary")