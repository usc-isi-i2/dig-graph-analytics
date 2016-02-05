import org.apache.hadoop.io.Text
import org.apache.spark.SparkConf
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx._
import org.apache.spark.graphx.{Graph, VertexId, Edge}
import org.apache.spark.{SparkContext, SparkConf}
import org.json4s.JsonAST.JString
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.hadoop.io.Text
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization

import scala.collection.mutable.ArrayBuffer
import scala.util.hashing.Hashing


/**
  * Created by rajagopal on 1/21/16.
  */
class GraphHelper() extends Serializable{

  def vertex_mapper(key : Text,value:Text):ArrayBuffer[(VertexId, String)] = {
    val JSON = parse(value.toString)
    //if 'a' is offer return json line with added type of Offer
    var map = collection.mutable.Map[String, Any]()
    implicit val formats = DefaultFormats

    val arr = new ArrayBuffer[(VertexId,String)]

    val temp1 = JSON \"name"
    val phone_nos = for { JString(x) <- temp1 } yield x

    for (phone_no <- phone_nos){
      map = map + ("value"->phone_no) + ("type"->"PhoneNumber")
      arr.+=((hash(phone_no),Serialization.write(map)))

    }

    val temp2 = JSON\"owner"
    val owner_uris = for { JString(x) <- temp2 } yield x

    for (owner_uri <- owner_uris){
      map = map + ("value"->owner_uri) + ("type"->"seller")
      arr.+=((hash(owner_uri),Serialization.write(map)))
    }

    arr
  }


  def edge_mapper(key : Text,value: Text) : ArrayBuffer[Edge[String]] = {

    val JSON = parse(value.toString)
    //if 'a' is offer return json line with added type of Offer
    val arr = new ArrayBuffer[Edge[String]]
    var map = collection.mutable.Map[String, Any]()
    implicit val formats = DefaultFormats

    val temp1 = JSON \"name"
    val phone_uris = for { JString(x) <- temp1 } yield x

    val temp2 = JSON\"owner"
    val owner_uris = for { JString(x) <- temp2 } yield x


    for (phone_uri <- phone_uris){
      for(owner_uri <- owner_uris){
        arr.+= (Edge(hash(phone_uri),hash(owner_uri),"ownerOf"))
      }
    }
    arr

  }

  def hash(str:String):Long={
    var hash = 7
    var i=0
    for(i<- 0 until str.length-1) {
      hash = hash * 31 + str.charAt(i)
    }
    math.abs(hash % 982451653)
  }

}
