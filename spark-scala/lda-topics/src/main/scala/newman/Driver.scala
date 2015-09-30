package newman

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.mllib.clustering.{LDA, DistributedLDAModel}
import org.apache.spark.mllib.linalg.{Vectors, Vector}

import org.apache.spark.rdd.RDD

import java.io.{InputStream, ByteArrayInputStream}
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets

import scala.io.Source._

import scala.util.parsing.json._

object Driver {

  val help = """
  | arguments: {input_path_dtm} {topic_scores_output} {vocab_index_file} {topic_map_output_file}
  | """.stripMargin

  def main(args: Array[String]):Unit = {

    if (args.length != 4) {
      println(help)
      System.exit(0)
    }

    val (input_path, hdfs_output_dir, vocab_index_path, topic_map_output_file) = (args(0), args(1), args(2), args(3))
    val spark_conf = new SparkConf().setAppName("newman lda topic clustering")
    val sc = new SparkContext(spark_conf)
    //override block size for localmode in MB
    sc.hadoopConfiguration.set("fs.local.block.size", (128 * 1024 * 1024).toString)

    val vocabIdx = fromFile(vocab_index_path).getLines.toList.map { 
      s => 
        s.split('\t').toList match { 
          case List(a,b) => (a.toInt, b) 
        } 
    }.toMap

    // input format "{uid}\t{scores seperated with <space>}"
    //( (id, counts), index )
    val data:RDD[((String, Vector), Long)] = sc.textFile(input_path).map { 
      line => line.split("\t").toList match { 
        case List(id, counts) => (id, Vectors.dense(counts.split(" ").map(_.toDouble))) 
      } 
    }.zipWithIndex.cache()
    
    val documents:RDD[(Long,Vector)] = data.map{ t => (t._2, t._1._2) }

    val ldaModel:DistributedLDAModel = new LDA().setK(10).setMaxIterations(100).run(documents) match { 
      case m: DistributedLDAModel => m 
    }

    val topics:Array[(Array[(String, Double)], Int)] = ldaModel.describeTopics(maxTermsPerTopic = 10).map{
      t => (t._1.map(vocabIdx).zip(t._2))
    }.zipWithIndex

    //json of topics 
    val topics_export:String = topics.map { 
      t => t match { 
        case (items, idx) => {
          val topic_items = (for { 
            ((term, score), i) <- items.zipWithIndex 
          } yield {
            JSONObject(Map("term" -> term, "score" -> score, "idx" -> i))
          }).toList
          JSONObject(Map("idx" -> idx, "topic" -> JSONArray(topic_items))).toString()
        }
      }
    }.toList.mkString("\n")

    Files.write(Paths.get(topic_map_output_file), topics_export.getBytes(StandardCharsets.UTF_8)) 

    val ids:RDD[(Long, String)] = data.map( t => (t._2, t._1._1))

    val topic_scores:RDD[String] = ids.join(ldaModel.topicDistributions).map { 
      t => t._2 match { 
        case (id:String, scores:Vector) => {
          "%s\t%s".format(id,scores.toArray.map { "%.5f" format _ }.mkString(" "))
        }
      }
    }

    topic_scores.saveAsTextFile(hdfs_output_dir)

  }

}

