package bdpuh.github

import scala.collection.mutable
import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.graphx.PartitionStrategy._
import org.apache.spark.SparkContext._

object Analytics extends Logging {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println(
        "Usage: Analytics <taskType> <file> --numEPart=<num_edge_partitions> [other options]")
      System.exit(1)
    }

    val taskType = args(0)
    val fname = args(1)
    val optionsList = args.drop(2).map { arg =>
      arg.dropWhile(_ == '-').split('=') match {
        case Array(opt, v) => (opt -> v)
        case _ => throw new IllegalArgumentException("Invalid argument: " + arg)
      }
    }
    val options = mutable.Map(optionsList: _*)
    val conf = new SparkConf().setMaster("local").set("spark.locality.wait", "100000")

    val numEPart = options.remove("numEPart").map(_.toInt).getOrElse {
      println("Set the number of edge partitions using --numEPart.")
      sys.exit(1)
    }
    val usersFile = options.remove("usersFile").getOrElse {
      println("Set the number of edge partitions using --numEPart.")
      sys.exit(1)
    }
    val partitionStrategy: Option[PartitionStrategy] = options.remove("partStrategy")
      .map(PartitionStrategy.fromString(_))
    val edgeStorageLevel = options.remove("edgeStorageLevel")
      .map(StorageLevel.fromString(_)).getOrElse(StorageLevel.MEMORY_ONLY)
    val vertexStorageLevel = options.remove("vertexStorageLevel")
      .map(StorageLevel.fromString(_)).getOrElse(StorageLevel.MEMORY_ONLY)

    taskType match {
      case "pagerank" =>
        val tol = options.remove("tol").map(_.toFloat).getOrElse(0.001F)
        val outFname = options.remove("output").getOrElse("")
        val numIterOpt = options.remove("numIter").map(_.toInt)

        options.foreach {
          case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
        }

        println("======================================")
        println("|             PageRank               |")
        println("======================================")

        val sc = new SparkContext(conf.setAppName("PageRank(" + fname + ")"))

        val unpartitionedGraph = GraphLoader.edgeListFile(sc, fname,
          minEdgePartitions = numEPart,
          edgeStorageLevel = edgeStorageLevel,
          vertexStorageLevel = vertexStorageLevel).cache()
        val graph = partitionStrategy.foldLeft(unpartitionedGraph)(_.partitionBy(_))

        val pr = (numIterOpt match {
          case Some(numIter) => PageRank.run(graph, numIter)
          case None => PageRank.runUntilConvergence(graph, tol)
        }).vertices.cache()

        val users = sc.textFile(usersFile).map { line =>
          val fields = line.split(" ")
          (fields(0).toLong, fields(1))
        }

        println("The top ten Github users are: (format - (PageRank, username))")
        val top10 = users.join(pr).map {
          case (id, (username, rank)) => (rank, username)
        }.sortByKey(false).take(10)
        println("The top ten Github users are: (format - (PageRank, username))")
        println(top10.mkString("\n"))

        sc.stop()

      case "sp" =>
        val sourceId = options.remove("sourceId").map(_.toLong).getOrElse {
          println("Set the sourceId using --sourceId.")
          sys.exit(1)
        }
        options.foreach {
          case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
        }

        println("======================================")
        println("| 	   Single Source Shortest Path    |")
        println("======================================")

        val sc = new SparkContext(conf.setAppName("SingleSourceShortestPath(" + fname + ")"))
        val unpartitionedGraph = GraphLoader.edgeListFile(sc, fname,
          minEdgePartitions = numEPart,
          edgeStorageLevel = edgeStorageLevel,
          vertexStorageLevel = vertexStorageLevel).cache()
        val graph = partitionStrategy.foldLeft(unpartitionedGraph)(_.partitionBy(_))
        val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) (0.0, List[VertexId](sourceId)) else (Double.PositiveInfinity, List[VertexId]()))
        val sssp = initialGraph.pregel((Double.PositiveInfinity, List[VertexId]()), Int.MaxValue, EdgeDirection.Out)(
          (id, dist, newDist) => if (dist._1 < newDist._1) dist else newDist,
          triplet => {
            if (triplet.srcAttr._1 + triplet.attr < triplet.dstAttr._1) {
              Iterator((triplet.dstId, (triplet.srcAttr._1 + triplet.attr, triplet.srcAttr._2 :+ triplet.dstId)))
            } else {
              Iterator.empty
            }
          },
          (a, b) => if (a._1 < b._1) a else b // Merge Message
          )
        val users = sc.textFile(usersFile).map { line =>
          val fields = line.split(" ")
          (fields(0).toLong, fields(1))
        }
        val sourceName = users.filter{ case(id, _) => id == sourceId }.collect.head._2
        val usersVerticesJoined = sssp.vertices.join(users)
        usersVerticesJoined.foreach {
          case (id, ((dist, ids), username)) => 
            if (!dist.isPosInfinity)
              println(sourceName + " -> " + ids.slice(1, ids.length).mkString(" -> "))
          }
        
        sc.stop()
  
      case _ =>
        println("Invalid task type.")
    }
  }
}
