package Inbound.hivetokafka

/**
  * Created by kalit_000 on 9/2/17.
  */

import org.apache.hadoop.fs.FileSystem
import java.io.{BufferedOutputStream, ByteArrayOutputStream, File, FileInputStream}
import java.util.{Properties, UUID}

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.apache.hadoop.fs.Path
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io._
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.{LogManager, PropertyConfigurator}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.apache.avro.SchemaBuilder
import org.apache.avro.SchemaBuilder._
import org.apache.spark.sql.Row
import com.databricks.spark.avro._
import org.apache.hadoop.hive.ql.io.parquet.convert.HiveSchemaConverter

import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.{Row, SparkSession}
import utils.DBUtils
import utils.ReportMail


case class Fields(name:String,`type`:String)
case class tblSchemaJson(namespace:String,`type`:String,name:String,fields:List[Fields])


object MessageProducer {

  var tblAvroSchmea:Schema=null;
  val logger=Logger.getLogger("MessageProducer")
  var msgCount:Int=0;

  val spark:SparkSession=SparkSession.builder().config("spark.files.overwrite",true).getOrCreate()
  val fs=FileSystem.get(spark.sparkContext.hadoopConfiguration);

  val conf=spark.sparkContext.getConf
  conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
  conf.set("spark.driver.allowMultipleContexts","true")
  val localSparkFileRoot=SparkFiles.getRootDirectory()
  val sparkFileRootDie="."


  def parseAvroData(rec:Row,colList:Array[String]):GenericRecord={

    val tblAvroRecord:GenericRecord =new GenericData.Record(tblAvroSchmea);
    var noOfColumns:Int=colList.size
    var rowNumber:Int=0;
    for (columnIndex <- 0 to noOfColumns -1) {
      tblAvroRecord.put(colList(columnIndex).toString,rec.getAs(colList(columnIndex)))
    }
    tblAvroRecord
  }

  def recSerializer(avroRec:GenericRecord):Array[Byte]={
    val writer=new SpecificDatumWriter[GenericRecord](tblAvroSchmea)
    val out=new ByteArrayOutputStream()
    val encoder:BinaryEncoder=EncoderFactory.get().binaryEncoder(out,null)
    writer.write(avroRec,encoder)
    encoder.flush()
    out.close()
    val serializedBytes:Array[Byte]=out.toByteArray()
    serializedBytes
  }

  def sendMsg(topic:String,producer:KafkaProducer[String,Array[Byte]],byteAvroMsg:Array[Byte]):Unit={
    try{
      producer.send(new ProducerRecord[String,Array[Byte]](topic,byteAvroMsg))
    } catch {
      case ex:Exception => {
        logger.info("Exception Message =========>" + ex.getMessage,ex)
      }
    } finally {
      msgCount +=1
    }
  }

  def setProducerConfigs(): java.util.Properties ={

    val producerConfig=new java.util.Properties()

    producerConfig.put("client.id",UUID.randomUUID().toString)
    producerConfig.put("request.required.acks","-1")
    producerConfig.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    producerConfig.put("value.serializer","org.apache.kafka.common.serialization.ByteArraySerializer")
    producerConfig.put("linger.ms","5")
    producerConfig.put("compression.type","snappy")
    producerConfig
  }


  def getReportMailSubject(envvalue:String,Jobname:String,srctablecount:Int,msgcount:Int):String={
    var messageSubject:String=null;
    if (srctablecount == msgcount){
      messageSubject=envvalue.toUpperCase() + ": #RECON- JOB_NAME : " + Jobname + " # was successful"
    } else {
      messageSubject=envvalue.toUpperCase() + ": #RECON- JOB_NAME : " + Jobname + " # was not successful"
    }
    messageSubject
  }



  def main(args: Array[String]): Unit = {

    val target_topic_name="test"
    val brokers="kafkabrokers"
    val bootstrap="kafkabrokers"
    val srcolsnames="srccolsnames"

    var avroRecord:GenericRecord=null
    //ADD producer configs
    var producerConfig=setProducerConfigs()

    producerConfig.put("metadata.broker.list",brokers)
    producerConfig.put("bootstrap.servers",bootstrap)

    val srcTable:String="blahhh"


    val srcTableDF=spark.read.format("jdbc").options(
      Map(
        "url" -> DBUtils.mysqlurl,
        "dbtable" -> srcTable,
        "user" -> DBUtils.mysql_username,
        "password" -> DBUtils.mysql_password,
        "driver" -> "com.mysql.jdbc.Driver",
        "fetchSize" -> "10000"
      )
    ).load()

   //val hiveTableDataDF=spark.sql("")

   val avroSchema=SchemaConverters.convertStructToAvro(srcTableDF.schema,SchemaBuilder.record(srcTable),"")
    println("New built Avro Schema using data bricks pacakge ---------->" + avroSchema)

    //schema generation
    var parser:Schema.Parser=new Schema.Parser();
    tblAvroSchmea=parser.parse(avroSchema.toString);

    //Record generation
    val srcTableCount=srcTableDF.count().toInt

    if(srcTableDF.rdd.isEmpty() || srcTableCount == 0){
      logger.error("Table : " + srcTable + "is not giving out records")
    } else {
      logger.info("Table : " + srcTable + " is having records")
    }

    val colNamesArr:Array[String]=srcTableDF.columns
    if (colNamesArr.size == 0){
      logger.error("Error in getting Column Information from Dataframe.")
    }

    var producer:KafkaProducer[String,Array[Byte]]=new KafkaProducer[String,Array[Byte]](producerConfig)

    val startTimeMillis=System.currentTimeMillis()

     srcTableDF.collect().foreach{ rec:Row => {
       var avroRecord: GenericRecord =parseAvroData(rec,colNamesArr);
       var byteAvro:Array[Byte]=recSerializer(avroRecord)
       sendMsg(target_topic_name,producer,byteAvro)
     }
     }

    val endTimeMillis=System.currentTimeMillis()
    val durationSeconds=(endTimeMillis-startTimeMillis)/1000

    logger.info("Time Taken to post the Messages into Topic ------> :" + durationSeconds)
    logger.info("Closing Producer for Topic : " + target_topic_name)
    producer.close()

    var messageSubject:String=getReportMailSubject("DEV","DimAccount",srcTableCount,msgCount)

    val messageHeader="<html><table style=\"width:100%\">";
    val messageTrail="</table></html>";
    var messageContent:String =messageHeader+"<tr><tc width=\"25%\"> Source Table Name : (" + srcTable + ") </td></tr>"


    ReportMail.sendMail("kali.tummala@gmail.com","kali.tummala@gmail.com",messageSubject,messageContent)

    logger.info("=====>Number of Messages Sent : " + msgCount)


  }



























}
