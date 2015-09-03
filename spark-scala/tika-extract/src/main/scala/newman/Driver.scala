package newman

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import java.io.{InputStream, ByteArrayInputStream}
import java.nio.file.{Files, Paths}

//java 8
//import java.util.Base64
import org.apache.commons.codec.binary.Base64;

//Tika
import org.apache.tika.Tika
import org.apache.tika.language.LanguageIdentifier
import org.apache.tika.metadata.Metadata
import org.apache.tika.parser.{AutoDetectParser, Parser, ParseContext}
import org.apache.tika.sax.BodyContentHandler

import org.xml.sax.ContentHandler

import scala.util.Try

import scala.io.Source._

import scala.util.parsing.json._

class CC[T] { def unapply (a:Any):Option[T] = Some(a.asInstanceOf[T]) }

object M extends CC[Map[String, Any]]
object LL extends CC[List[Any]]
object S extends CC[String]
object D extends CC[Double]
object I extends CC[Int]
object L extends CC[Long]
object B extends CC[Boolean]

object Driver {

  def extractInfo(base64Str:String): (String, Boolean) = {
    val p:Parser = new AutoDetectParser()
    val handler:BodyContentHandler = new BodyContentHandler(-1)
    val byte_buff = Base64.decodeBase64(base64Str)
    val is:InputStream =  new ByteArrayInputStream(byte_buff)
    var t:Throwable = null
    try{
      p.parse(is, handler, new Metadata(), new ParseContext())
      (handler.toString,  true)
    } catch {
      //edge case of 2 different versions of commons-compress on classpath
      //which I haven't been able to figure out how to override with spark
      case n:NoSuchMethodError => (n.toString, false)
      case x:Throwable => t = x; throw x
    } finally {
      if (is != null) {
        try {
          is.close()
        } catch {
          case y:Throwable => t.addSuppressed(y)
        }
      } else {
        is.close()
      }
    }
  }

  def extractinfo(base64Str: String): Try[(String, Boolean)] = Try(extractInfo(base64Str))

// { 
//   "filename" : fileName,
//   "guid" : filename_guid,
//   "extension" : extension,
//   "contents64" : b64
// }

// 
// simple JSON parsing without 3rd party libraries 
// http://stackoverflow.com/a/4186090/3438870

  def extract(jsonString: String, validExts: List[String]) = {
    JSON.perThreadNumberParser = {_.toLong}
    JSON.parseFull(jsonString) match {
      case Some(M(map)) => {
        val attachments_list = for {
          M(m) <- List(map)
          S(id) = m("id")
          LL(attachments) = m("attachments")
          M(attach) <- attachments
          S(fileguid) = attach("guid")
          S(extension) = attach("extension")
          S(base64Str) = attach("contents64")
          //filter extension list
          if validExts.contains(extension.replace(".", "").toLowerCase)
        } yield {
          try {
            var (content, extracted) = extractinfo(base64Str).getOrElse(("", false))
            (id, Map("guid" -> fileguid, "content" -> content, "content_extracted" -> extracted))
          } catch {
            case t: Throwable => {
              t.addSuppressed(new Throwable(s"Exception on $fileguid"))
              throw t
            }
          }
        }
        if (attachments_list.isEmpty) {
          List.empty
        } else {
          val msg_id = attachments_list.head._1
          List(List(msg_id, JSONArray(attachments_list.map(t => JSONObject(t._2))).toString()).mkString("\t"))
        }
      }
      case None => List.empty
    }
  }

  val help = """
         | arguments: {input_path_walk_parts} {hdfs_output_dir} {valid_exts_file}
         | """.stripMargin

  def main(args: Array[String]):Unit = {
    if (args.length != 3) {
      println(help)
      System.exit(0)
    }
    val (input_path, hdfs_dir, valid_exts_file) = (args(0), args(1), args(2))
    val spark_conf = new SparkConf().setAppName("Newman attachment text extract")
    val sc = new SparkContext(spark_conf)
    //override block size for localmode in MB
    sc.hadoopConfiguration.set("fs.local.block.size", (128 * 1024 * 1024).toString)
    val valid_exts = fromFile(valid_exts_file).getLines.toList
    println(s"Extension filter: $valid_exts")
    val distData = sc.textFile(input_path)
    distData.flatMap( line => extract(line.trim, valid_exts)).saveAsTextFile(hdfs_dir)
  }

}

