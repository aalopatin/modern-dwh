package io.github.aalopatin.mdwh.sensor

import com.typesafe.config.ConfigFactory
import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.github.aalopatin.mdwh.sensor.config.{MeasurementConfig, SensorSimulator}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.nio.file.{Files, Paths, StandardOpenOption}
import java.time.Instant
import java.util.Properties
import scala.collection.mutable
import scala.jdk.CollectionConverters.*
import scala.util.Random

object Main:

  private val Config = ConfigFactory.load()

  private val Topic = sys.env.getOrElse("KAFKA_TOPIC", throw IllegalArgumentException("KAFKA_TOPIC isn't defined!"))
  private val BootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", throw IllegalArgumentException("KAFKA_BOOTSTRAP_SERVERS isn't defined!"))
  private val SchemaRegistryUrl = sys.env.getOrElse("KAFKA_SCHEMA_REGISTRY_URL", throw IllegalArgumentException("KAFKA_SCHEMA_REGISTRY_URL isn't defined!"))

  private val StringSchema =
    """
      |{
      |  "type": "record",
      |  "name": "SensorData",
      |  "fields": [
      |    {
      |      "name": "id",
      |      "type": "string"
      |    },
      |    {
      |       "name": "measurements_timestamp",
      |       "type": {
      |         "type": "long",
                      "logicalType": "timestamp-millis"
      |       }
      |    },
      |    {
      |      "name": "measurements",
      |      "type": {
      |        "type": "array",
      |        "items": {
      |          "name": "Measurement",
      |          "type": "record",
      |          "fields": [
      |            {
      |              "name": "name",
      |              "type": "string"
      |            },
      |            {
      |              "name": "value",
      |              "type": "double"
      |            }
      |          ]
      |        }
      |      }
      |    }
      |  ]
      |}
      |
      |""".stripMargin

  private val parser = new Schema.Parser
  val schema: Schema = parser.parse(StringSchema)

  if (!Files.exists(Paths.get(checkpointsDirectory))) {
    Files.createDirectory(Paths.get(checkpointsDirectory))
  }

  def main(args: Array[String]): Unit =

    assert(args.length == 1, "Count of arguments should be 1")

    implicit val id: String = args(0)
    val (frequency, measurements) = getConfigs(args(0))

    val kafkaProperties = new Properties()
    kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BootstrapServers)
    kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer].getName)
    kafkaProperties.put("schema.registry.url", SchemaRegistryUrl)

    val producer = new KafkaProducer[String, GenericData.Record](kafkaProperties)

    val measurementSchema = schema.getField("measurements").schema().getElementType

    val random = new Random()

    var isRealtime = false
    var (lastEventTime, lastMeasurements) = loadCheckpointFromFile

    while(true) {

      val eventTime = if(isRealtime) {
        Instant.now()
      } else {
        lastEventTime.plusSeconds(frequency)
      }

      val message = new GenericData.Record(schema)
      message.put("id", id)
      message.put("measurements_timestamp", eventTime.toEpochMilli)

      val measurementsRecords = measurements.map { measurement =>
        val measurementRecord = new GenericData.Record(measurementSchema)
        measurementRecord.put("name", measurement.name)

        lastMeasurements(measurement.name) = measurement.simulator.nextValue(lastMeasurements.get(measurement.name))

        measurementRecord.put("value", lastMeasurements(measurement.name))
        measurementRecord
      }.asJava

      message.put(
        "measurements",
        measurementsRecords
      )

      val record = new ProducerRecord(Topic, id, message)

      producer.send(record, new ProducerCallback())

      if (isRealtime || eventTime.plusSeconds(frequency).isAfter(Instant.now())) {
        isRealtime = true
        lastEventTime = eventTime
        Thread.sleep(frequency * 1000)
      } else {
        lastEventTime = eventTime
      }

      saveCheckpoint(lastEventTime, lastMeasurements)

    }

  private def getConfigs(id: String) =

    val frequency = Config.getInt(s"$id.frequency")

    val measurements = Config.getConfigList(s"$id.measurements").asScala.map { measurement =>
      MeasurementConfig(
        measurement.getString("name"),
        SensorSimulator(
          measurement.getDouble("min"),
          measurement.getDouble("max"),
          measurement.getDouble("step"),
          measurement.getInt("scale")
        )

      )

    }.toList

    (frequency, measurements)

  private def checkpointsDirectory = "checkpoints"

  private def checkpointFilePath(implicit id: String) = Paths.get(s"$checkpointsDirectory/$id")

  private def loadCheckpointFromFile(implicit id: String): (Instant, mutable.Map[String, Double]) = {
    val filePath = checkpointFilePath
    if (!Files.exists(filePath)) {
      val initialTimestamp = Instant.parse(Config.getString("timestamp-backfill"))
      (initialTimestamp, mutable.Map.empty)
    } else {
      val lines = Files.readAllLines(filePath).asScala
      var timestampOpt: Option[Instant] = None
      val measurements = mutable.Map.empty[String, Double]

      lines.foreach { line =>
        val Array(key, value) = line.split("=", 2)
        if (key == "timestamp") {
          timestampOpt = Some(Instant.parse(value))
        } else {
          measurements(key) = value.toDouble
        }
      }

      (timestampOpt.getOrElse(Instant.parse(Config.getString("timestamp-backfill"))), measurements)
    }
  }

  private def saveCheckpoint(lastEventTime: Instant, lastMeasurements: mutable.Map[String, Double])(implicit id: String): Unit = {
    val filePath = checkpointFilePath
    val tmpFile = Paths.get(filePath.toString + ".tmp")

    val lines = ("timestamp=" + lastEventTime.toString) +: lastMeasurements.map { case (k, v) => s"$k=$v" }.toSeq

    Files.write(
      tmpFile,
      lines.mkString("\n").getBytes,
      StandardOpenOption.CREATE,
      StandardOpenOption.TRUNCATE_EXISTING
    )

    Files.move(
      tmpFile,
      filePath,
      java.nio.file.StandardCopyOption.REPLACE_EXISTING,
      java.nio.file.StandardCopyOption.ATOMIC_MOVE
    )
  }
