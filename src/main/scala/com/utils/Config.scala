package com.utils

import scala.util.Try

object InputType extends Enumeration {
  type InputType = Value
  val Csv, Parquet = Value
}
import InputType._

case class Config(eventsInputPath: String, purchasesInputPath: String, outputBasePath: String, inputType: InputType)

object Config {
  def parseFromCommandLine(cliArguments: Array[String]): Config = {
    val eventsInputPath = Try { cliArguments(0) } getOrElse "src/main/resources/mobile_app_clickstream/"
    val purchasesInputPath = Try { cliArguments(1) } getOrElse "src/main/resources/user_purchases/"
    val outputBasePath = Try { cliArguments(2) } getOrElse "src/main/resources/out/"
    val inputType = Try { cliArguments(3) } getOrElse "Csv" match {
      case "Parquet" => Parquet
      case "parquet" => Parquet
      case _ => Csv
    }
    Config(eventsInputPath, purchasesInputPath, outputBasePath, inputType)
  }
}