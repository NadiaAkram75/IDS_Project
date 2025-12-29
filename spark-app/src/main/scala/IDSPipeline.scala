import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.types._
import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.URI

object IDSPipeline {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("IDS-ML-Pipeline")
      .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
      .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
      .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2") 
      .getOrCreate()

    import spark.implicits._

    // --- BUCKET INITIALIZATION ---
    val sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    val fs = FileSystem.get(new URI("s3a://ids-bucket"), conf)
    val bucketPath = new Path("s3a://ids-bucket/")
    
    if (!fs.exists(bucketPath)) {
      println("Bucket 'ids-bucket' not found. Creating it now...")
      fs.mkdirs(bucketPath)
    }

    // 1. Schema matching the Producer JSON
    val schema = new StructType()
      .add("duration", IntegerType)
      .add("protocol_type", StringType)
      .add("src_bytes", IntegerType)
      .add("dst_bytes", IntegerType)
      .add("label", IntegerType)

    // 2. Training Phase
    println("Loading training data...")
    val trainingData = spark.read.option("header", "false").option("inferSchema", "true")
      .csv("/data/KDDTrain+.txt")
      .toDF("duration", "protocol_type", "service", "flag", "src_bytes", "dst_bytes", "land", "wrong_fragment", "urgent", "hot", "num_failed_logins", "logged_in", "num_compromised", "root_shell", "su_attempted", "num_root", "num_file_creations", "num_shells", "num_access_files", "num_outbound_cmds", "is_host_login", "is_guest_login", "count", "srv_count", "serror_rate", "srv_serror_rate", "rerror_rate", "srv_rerror_rate", "same_srv_rate", "diff_srv_rate", "srv_diff_host_rate", "dst_host_count", "dst_host_srv_count", "dst_host_same_srv_rate", "dst_host_diff_srv_rate", "dst_host_same_src_port_rate", "dst_host_srv_diff_host_rate", "dst_host_serror_rate", "dst_host_srv_serror_rate", "dst_host_rerror_rate", "dst_host_srv_rerror_rate", "label_name", "difficulty")

    val formattedTrain = trainingData.withColumn("label", when($"label_name" === "normal", 0).otherwise(1))

    val assembler = new VectorAssembler()
      .setInputCols(Array("duration", "src_bytes", "dst_bytes"))
      .setOutputCol("features")
      .setHandleInvalid("skip") // Defensive: skip invalid rows during training

    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(10)

    val pipeline = new Pipeline().setStages(Array(assembler, rf))
    
    println("Starting model training...")
    val model = pipeline.fit(formattedTrain)
    
    model.write.overwrite().save("s3a://ids-bucket/models/rf-model")
    println("Model trained and saved to MinIO!")

    // 3. Streaming Phase
    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:29092") 
      .option("subscribe", "network_traffic")
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false") 
      .load()

    val liveTraffic = kafkaStream.selectExpr("CAST(value AS STRING) as json")
      .select(from_json($"json", schema).as("data"))
      .select("data.*")
      .withColumn("timestamp", current_timestamp())
      // CRITICAL FIX: Drop nulls here so VectorAssembler doesn't crash the stream
      .na.drop(Array("duration", "src_bytes", "dst_bytes"))

    val predictions = model.transform(liveTraffic)

    // --- BLOCKER LOGIC ---
    val blockedTraffic = predictions.filter($"prediction" === 1.0)
    
    // 4. Outputs

    // Console: Blocked Attacks alerts
    blockedTraffic.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .start()

    // Console: Live Accuracy calculation
    predictions
      .withColumn("isCorrect", when($"label" === $"prediction", 1).otherwise(0))
      .agg(avg("isCorrect").as("live_accuracy"))
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()

    // Storage: Dashboard files (Streamlit)
    predictions.writeStream
      .format("parquet")
      .option("path", "s3a://ids-bucket/predictions")
      .option("checkpointLocation", "s3a://ids-bucket/checkpoints/streaming")
      .start()

    // Storage: Quarantine log
    blockedTraffic.writeStream
      .format("parquet")
      .option("path", "s3a://ids-bucket/quarantine")
      .option("checkpointLocation", "s3a://ids-bucket/checkpoints/blocker")
      .start()

    spark.streams.awaitAnyTermination()
  }
}