// Databricks notebook source
dbutils.fs.mkdirs("/FileStore/demo_quality/config")

// COMMAND ----------

dbutils.fs.mkdirs("/FileStore/demo_quality/titanic")

// COMMAND ----------

// MAGIC %sh
// MAGIC cat /dbfs/FileStore/demo_quality/titanic/titanic.csv

// COMMAND ----------

//dbutils.fs.help()
val arr = dbutils.fs.ls("/FileStore/demo_quality/config")
arr.foreach(fi => {println(fi.path)})
dbutils.fs.mkdirs("/FileStore/demo_quality/config")
dbutils.fs.ls("/FileStore/demo_quality/config")
arr.foreach(fi => {println(fi.path)})
val cfgchkdata = """
col_name,data_type,fmt_check,fmt_regex,data_type_check,null_check,empty_check,dup_check,range_check,range_check_values,const_check,const_check_values
survived,Int,1,"^[0-9]*$",1,1,1,0,1,"0-1",1,"0,1"
pclass,Int,1,"^[0-9]*$",1,1,1,0,1,"1-3",1,"1,2,3"
name,String,1,"^[a-zA-Z][a-zA-Z\. ]+$",1,1,1,0,0,"",0,""
sex,Int,1,"^[0-9]*$",1,1,1,0,0,"",0,""
age,Int,1,"^[0-9]*$",1,1,1,0,0,"",0,""
siblings,Int,1,"^[0-9]*$",1,1,1,0,1,"0-1",1,"0,1"
parents,Int,1,"^[0-9]*$",1,1,1,0,1,"0-1",1,"0,1"
fare,Double,1,"^[0-9]*$",1,1,1,0,0,"",0,""
"""
////println(cfgchkdata)
dbutils.fs.rm("/FileStore/demo_quality/config/titanic.cfg")
dbutils.fs.put("/FileStore/demo_quality/config/titanic.cfg", cfgchkdata)
dbutils.fs.ls("/FileStore/demo_quality/config")
arr.foreach(fi => {println(fi.path)})

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

object QualityHelper extends Serializable {
  
  //
  var configChecksDfVar: DataFrame = null
  var dataDfVar: DataFrame = null
  val configTempView = "config"
  val dataTempView = "data"
  
  
  //
  def readConfigChecksFile(path: String): Unit = {
    //
    val file_type = "csv"
    val infer_schema = false
    val first_row_is_header = true
    val delimiter = ","
    //
    val configdf =
      spark
      .read
      .format(file_type)
      .option("inferSchema", infer_schema)
      .option("header", first_row_is_header)
      .option("sep", delimiter)
      .option("quote","\"")
      .load(path)
    
    //
    configChecksDfVar = configdf
  }
  
  
  //
  def readDataFile(path: String): Unit = {
    //
    val file_type = "csv"
    val infer_schema = false
    val first_row_is_header = true
    val delimiter = ","
    //
    val datadf =
      spark
      .read
      .format(file_type)
      .option("inferSchema", infer_schema)
      .option("header", first_row_is_header)
      .option("sep", delimiter)
      .option("quote","\"")
      .load(path)
    
    //
    dataDfVar = datadf
  }
  
  
  //
  def init(cfgPath: String, dataPath: String): Unit = {
    //
    readConfigChecksFile(cfgPath)
    readDataFile(dataPath)
  }
  
  
  //
  def getConfigChecks(): Array[Row] = {
    //
    configChecksDfVar.rdd.collect()
  }
  
  

  //
  def getConfigKeyColumns(): DataFrame = {
    //
    configChecksDfVar.select("col_name")
  }
  
  
  
  //
  def addColumnValidatorStats(): DataFrame = {
    
    //
    var addColStatDfVar: DataFrame = dataDfVar
    val configArr: Array[Row] = getConfigChecks()
    
    //
    def fmtCheck = udf((row: Row) => {
      
      //
      val chkCols = configArr.filter(p => "1".equals(p.getAs[String]("fmt_check")))
      var colList = ""

      //
      chkCols.map(chkCol => {
        val colName = chkCol.getAs[String]("col_name")
        val mtchpat = chkCol.getAs[String]("fmt_regex")
        println(s"checking... $colName $mtchpat")
        val mtchr = mtchpat.r
        try {
          if (!row.getAs[String](colName).matches(mtchpat)) {
            if (colList.length > 0) colList += "~" + colName else colList += colName
          }
        } catch {
          case x: Throwable => if (colList.length > 0) colList += "~" + colName else colList += colName
        }
      })
      //
      colList
    })
      

    //
    def dataTypeCheck = udf((row: Row) => {
      
      //
      val chkCols = configArr.filter(p => "1".equals(p.getAs[String]("data_type_check")))
      var colList = ""
      
      //
      chkCols.map(chkCol => {
        val colName = chkCol.getAs[String]("col_name")
        val dataTypeName = chkCol.getAs[String]("data_type")
        
        //
        try {
          dataTypeName match {
            case "Int" => if (row.getAs(colName).toString.equals(row.getAs[Int](colName).toString.toInt)) row.getAs[Int](colName).getClass
            case "Long" => if (row.getAs(colName).toString.equals(row.getAs[Long](colName).toString.toLong)) row.getAs[Long](colName).getClass
            case "String" => if (row.getAs(colName).toString.equals(row.getAs[String](colName).toString)) row.getAs[String](colName).getClass
            case "Float" => if (row.getAs(colName).toString.equals(row.getAs[Float](colName).toString.toFloat)) row.getAs[Float](colName).getClass
            case "Double" => if (row.getAs(colName).toString.equals(row.getAs[Double](colName).toString.toDouble)) row.getAs[Double](colName).getClass
            case _ => ""
          }
        } catch {
          case x: Throwable => if (colList.length > 0) colList += "~" + colName else colList += colName
        }
        
      })
      //
      colList
    })
    
    
    //
    def nullCheck = udf((row: Row) => {
      
      //
      val chkCols = configArr.filter(p => "1".equals(p.getAs[String]("null_check")))
      var colList = ""

      //
      chkCols.map(chkCol => {
        val colName = chkCol.getAs[String]("col_name")
        if (row.getAs[String](colName) == null) {
          if (colList.length > 0) colList += "~" + colName else colList += colName
        }
      })
      //
      colList
    })
      

    //
    def emptyCheck = udf((row: Row) => {
      
      //
      val chkCols = configArr.filter(p => "1".equals(p.getAs[String]("empty_check")))
      var colList = ""

      //
      chkCols.map(chkCol => {
        val colName = chkCol.getAs[String]("col_name")
        if (row.getAs[String](colName) == "") {
          if (colList.length > 0) colList += "~" + colName else colList += colName
        }
      })
      //
      colList
    })
      
    
    //
    addColStatDfVar = dataDfVar
      .withColumn("fmtchkstat", fmtCheck(struct(dataDfVar.columns.map(col): _*)))
      .withColumn("datatypechkstat", dataTypeCheck(struct(dataDfVar.columns.map(col): _*)))
      .withColumn("nullchkstat", nullCheck(struct(dataDfVar.columns.map(col): _*)))
      .withColumn("emptychkstat", emptyCheck(struct(dataDfVar.columns.map(col): _*)))
    
    //
    addColStatDfVar
  }
  
  
  //
  //   def sumUpColStats(statDf: DataFrame, inputDf: DataFrame) = {
  //     var sumUpDfVar: DataFrame = statDf
  def sumUpColStats(colStatDf: DataFrame) = {
    
    //
    var sumUpDfVar: DataFrame = colStatDf
    val configArr: Array[Row] = getConfigChecks()
    
    //
    val totalRecCount = dataDfVar.count
    var mutColChkMap = mutable.Map[String, mutable.Map[String, Long]]()
    
    //
    val sumColMap = Map(
      "sum_fmt_chk_cnt" -> "fmtchkstat",
      "sum_type_chk_cnt" -> "datatypechkstat",
      "sum_null_chk_cnt" -> "nullchkstat",
      "sum_empty_chk_cnt" -> "emptychkstat"
    )
    
    //
    sumColMap.foreach(sumCol => {
      configArr.foreach(chkCol => {
        val colName = chkCol.getAs[String]("col_name").toString
        val colCnt = colStatDf.filter(col(sumCol._2).contains(colName)).count
        var inMap = mutColChkMap.getOrElse(sumCol._1, mutable.Map[String, Long]())
        inMap += colName -> colCnt.toLong
        mutColChkMap += sumCol._1 -> inMap
      })
    })
    //
    val immColChkMap = mutColChkMap.map(f => f._1 -> f._2.toMap).toMap
    
    //
    val keyConfDf = getConfigKeyColumns()
    val sumChkStatInitDf = immColChkMap.keys.foldLeft(keyConfDf)((keyConfDf, colNm) => keyConfDf.withColumn(colNm, lit("0")))
    
    //
    def replaceChkVal = udf((colNm: String, colGrp: String) => {
      immColChkMap.getOrElse(colGrp, mutable.Map[String, Long]()).getOrElse(colNm, "0".toLong)
    })
    
    //
    sumUpDfVar = immColChkMap.keys.foldLeft(sumChkStatInitDf)((sumChkStatInitDf, colNm) => sumChkStatInitDf.withColumn(colNm, replaceChkVal(col("col_name"), lit(colNm)) ))
    
    
    //
    sumUpDfVar
  }
    
    
}

// COMMAND ----------

QualityHelper.init("/FileStore/demo_quality/config/titanic.cfg", "/FileStore/demo_quality/titanic/titanic.csv")
val chkStatDf = QualityHelper.addColumnValidatorStats()
val qualityDf = QualityHelper.sumUpColStats(chkStatDf)
qualityDf.createOrReplaceTempView("quality_stat")
qualityDf.show()

// COMMAND ----------

// MAGIC %sql
// MAGIC select metric, col_name, sum(val) value from (
// MAGIC select 'empty' metric, sum_empty_chk_cnt val, col_name from quality_stat
// MAGIC union all
// MAGIC select 'fmt' metric, sum_fmt_chk_cnt val, col_name from quality_stat
// MAGIC union all
// MAGIC select 'null' metric, sum_null_chk_cnt val, col_name from quality_stat
// MAGIC union all
// MAGIC select 'type' metric, sum_type_chk_cnt val, col_name from quality_stat
// MAGIC ) 
// MAGIC group by col_name, metric
