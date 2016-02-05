/**
  * Created by rajagopal on 1/21/16.
  */

import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Text
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx._
import org.apache.spark.graphx.{Edge, VertexId, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable.ArrayBuffer

object Test {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("graphx Application").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //val filePath = "/Users/rajagopal/Desktop/github_repos/dig-graphx/sample-files/seq-files/Offer"
    val filePath = args(0)
    val offerRDD = sc.sequenceFile(filePath,classOf[Text],classOf[Text])

    // create vertexRDDs and edgeRDDs in the format required
    val helper = new GraphHelper

    val vertexRDD : RDD[(VertexId,String)]= offerRDD.flatMap(line => helper.vertex_mapper(line._1,line._2))
    //    vertexRDD.foreach(line => println(line))

    val edgeRDD : RDD[Edge[String]] = offerRDD.flatMap(line => helper.edge_mapper(line._1,line._2))

    //    edgeRDD.foreach(line => println(line))

    //    vertexRDD.saveAsTextFile(args(1))
    //    edgeRDD.saveAsTextFile(args(2))


    //    // create graph and compute connected components
    val graph = Graph(vertexRDD,edgeRDD)
    val cc = graph.connectedComponents().vertices
    val ranks = graph.pageRank(0.001).vertices

    val newRanks = ranks.mapValues(rank =>
      BigDecimal(rank).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble )

    val temp1 = cc.join(newRanks)

    val temp2 = temp1.join(vertexRDD).map{
      case((id1,((smallid,rank),attr)))=>
            var JSON = parse(attr)
            // add pagerank to the line
            val rankStr = """{ "pagerank" :  """ + rank.toString + "}"
            JSON = JSON merge parse(rankStr)
            var str = compact(render(JSON))
            (str,smallid)
        }

    val rev = temp2.map{
          case(u,v)=>(v,u)
    }

    val finalRDD = rev.groupByKey()


    val res = finalRDD.map{
          case(id,line)=>
            val arr = line.toArray
            val phone_nos = new ArrayBuffer[Tuple2[String,String]]()
            for(str <- arr){
              val JSON = parse(str)
              val attr = compact(render(JSON\"type"))
              if(attr.equals("\"PhoneNumber\""))
                phone_nos.+=((compact(render(JSON\"value")),compact(render(JSON\"pagerank"))))
            }
            if(phone_nos.length>2)
              phone_nos.mkString(",")
            else
              None
        }
//        res.foreach(line => println(line))
        res.saveAsTextFile(args(1))

  }

}
