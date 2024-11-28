import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig, duration2JavaDuration}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import scala.collection.JavaConverters._
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres
import java.sql.{Connection, DriverManager, Statement, SQLException}
import org.scalatest.funsuite.AnyFunSuite
import org.sunbird.obsrv.connector._
import org.sunbird.obsrv.connector.source._
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.sunbird.obsrv.job.util._
import scala.concurrent.duration._
import net.manub.embeddedkafka.Codecs._
import java.time.Duration
import java.util.{Properties, Collections}
import org.scalatest.matchers.should.Matchers

class JDBCConnectorTest extends AnyFunSuite with Matchers {

  var metric: Map[String,Any] = Map.empty.withDefaultValue(null)
  var d1Events: List[String] = null
  var d2Events: List[String] = null

  def setUpServices():Unit= {
    try {
      val jdbcConfig: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
      val postgresConfig = PostgresConnectionConfig(
        user = jdbcConfig.getString("postgres.user"),
        password = jdbcConfig.getString("postgres.password"),
        database = "postgres",
        host = jdbcConfig.getString("postgres.host"),
        port = jdbcConfig.getInt("postgres.port"),
        maxConnections = jdbcConfig.getInt("postgres.maxConnections")
      )
      val postgres = EmbeddedPostgres.builder.setPort(5432).start()
      val postgresConnect = new PostgresConnect(postgresConfig)
      val url = s"jdbc:postgresql://${postgresConfig.host}:${5432}/${postgresConfig.database}?user=${postgresConfig.user}&password=${postgresConfig.password}"
      val connection: Connection = DriverManager.getConnection(url)
      val st: Statement = connection.createStatement()
      createSchema(st)
      val args: Array[String] = Array("-f", "src/test/resources/test.conf", "-c", "nyt-psql.1")
      val jdbc: JDBCSourceConnector = new JDBCSourceConnector
      implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 2181)
      EmbeddedKafka.start()(config)
      SourceConnector.process(args, jdbc)
      d1Events = EmbeddedKafka.consumeNumberMessagesFrom[String]("connector.metrics", 1, timeout = 30.seconds)
      val result: Map[String, Any] = JSONUtil.deserialize[Map[String, Any]](d1Events.head)
      val rel1 = JSONUtil.serialize(result.get("edata").get)
      val result2:Map[String, Any] = JSONUtil.deserialize[Map[String, Any]](rel1)
      val rel3 = JSONUtil.serialize(result2.get("metric").get)
      metric=JSONUtil.deserialize[Map[String, Any]](rel3)
      println("\nMetric: ")
      d1Events.foreach(i => println(i))
      d2Events = EmbeddedKafka.consumeNumberMessagesFrom[String]("dev.ingest", 30, timeout = 30.seconds)
    }
    catch {
      case e: Exception => e.printStackTrace()
    }
  }

  test("Connector Metrics Count"){
    setUpServices()
    d1Events.size should be(1)
  }

  test("Ingest topic size"){
    d2Events.size should be(30)
  }

  test("Successful Records Count"){
    metric.get("success_records_count").get.asInstanceOf[Int] should be(30)
  }

  test("Failed Records Count") {
    metric.get("failed_records_count").get.asInstanceOf[Int] should be(0)
  }

  test("Total Records Count"){
    metric.get("total_records_count").get.asInstanceOf[Int] should be(30)
  }

  def createSchema(st:Statement): Unit = {
    val createDatasets: String = "CREATE TABLE datasets (\n\tid text NOT NULL,\n\tdataset_id text NULL,\n\t\"type\" text NOT NULL,\n\t\"name\" text NULL,\n\tvalidation_config json NULL,\n\textraction_config json NULL,\n\tdedup_config json NULL,\n\tdata_schema json NULL,\n\tdenorm_config json NULL,\n\trouter_config json NULL,\n\tdataset_config json NULL,\n\ttags _text NULL,\n\tdata_version int4 NULL,\n\tstatus text NULL,\n\tcreated_by text NULL,\n\tupdated_by text NULL,\n\tcreated_date timestamp DEFAULT now() NOT NULL,\n\tupdated_date timestamp NOT NULL,\n\tpublished_date timestamp DEFAULT now() NOT NULL,\n\tapi_version varchar(255) DEFAULT 'v1'::character varying NOT NULL,\n\t\"version\" int4 DEFAULT 1 NOT NULL,\n\tsample_data json DEFAULT '{}'::json NULL,\n\tentry_topic text DEFAULT '{{ .Values.global.env }}.ingest'::text NOT NULL,\n\tCONSTRAINT datasets_pkey PRIMARY KEY (id)\n);"
    val createConnectorRegistry: String = "CREATE TABLE connector_registry (\n\tid text NOT NULL,\n\tconnector_id text NOT NULL,\n\t\"name\" text NOT NULL,\n\t\"type\" text NOT NULL,\n\tcategory text NOT NULL,\n\t\"version\" text NOT NULL,\n\tdescription text NULL,\n\ttechnology text NOT NULL,\n\truntime text NOT NULL,\n\tlicence text NOT NULL,\n\t\"owner\" text NOT NULL,\n\ticonurl text NULL,\n\tstatus text NOT NULL,\n\tui_spec json DEFAULT '{}'::json NOT NULL,\n\tsource_url text NOT NULL,\n\t\"source\" json NOT NULL,\n\tcreated_by text NOT NULL,\n\tupdated_by text NOT NULL,\n\tcreated_date timestamp NOT NULL,\n\tupdated_date timestamp NOT NULL,\n\tlive_date timestamp NULL,\n\tCONSTRAINT connector_registry_connector_id_version_key UNIQUE (connector_id, version),\n\tCONSTRAINT connector_registry_pkey PRIMARY KEY (id)\n);"
    val createConnectorInstance: String = "CREATE TABLE connector_instances (\n\tid text NOT NULL,\n\tdataset_id text NOT NULL,\n\tconnector_id text NOT NULL,\n\tdata_format text NULL,\n\tconnector_config text NOT NULL,\n\toperations_config json NOT NULL,\n\tstatus text NOT NULL,\n\tconnector_state json DEFAULT '{}'::json NOT NULL,\n\tconnector_stats json DEFAULT '{}'::json NOT NULL,\n\tcreated_by text NOT NULL,\n\tupdated_by text NOT NULL,\n\tcreated_date timestamp NOT NULL,\n\tupdated_date timestamp NOT NULL,\n\tpublished_date timestamp NOT NULL,\n\tCONSTRAINT connector_instances_pkey PRIMARY KEY (id),\n\tCONSTRAINT connector_instances_connector_id_fkey FOREIGN KEY (connector_id) REFERENCES connector_registry(id),\n\tCONSTRAINT connector_instances_dataset_id_fkey FOREIGN KEY (dataset_id) REFERENCES datasets(id)\n);"
    val createSampleData: String ="CREATE TABLE new_york_taxi_data (\n\t\"VendorID\" int4 NULL,\n\ttpep_pickup_datetime timestamp NULL,\n\ttpep_dropoff_datetime timestamp NULL,\n\tpassenger_count int4 NULL,\n\ttrip_distance float8 NULL,\n\t\"RatecodeID\" int4 NULL,\n\tstore_and_fwd_flag text NULL,\n\t\"PULocationID\" int4 NULL,\n\t\"DOLocationID\" int4 NULL,\n\tpayment_type int4 NULL,\n\tfare_amount float8 NULL,\n\textra float8 NULL,\n\tmta_tax float8 NULL,\n\ttip_amount float8 NULL,\n\ttolls_amount float8 NULL,\n\timprovement_surcharge float8 NULL,\n\ttotal_amount float8 NULL,\n\tcongestion_surcharge float8 NULL,\n\ttripid varchar(50) NULL,\n\tprimary_passenger__email varchar(50) NULL,\n\tprimary_passenger__mobile varchar(50) NULL,\n\tfare_details__fare_amount float4 NULL,\n\tfare_details__extra int4 NULL,\n\tfare_details__mta_tax float4 NULL,\n\tfare_details__tip_amount int4 NULL,\n\tfare_details__tolls_amount int4 NULL,\n\tfare_details__improvement_surcharge float4 NULL,\n\tfare_details__total_amount float4 NULL,\n\tfare_details__congestion_surcharge varchar(50) NULL\n);"
    st.executeUpdate(createDatasets)
    st.executeUpdate(createConnectorRegistry)
    st.executeUpdate(createConnectorInstance)
    st.executeUpdate(createSampleData)
    insertData(st)
  }

  def insertData(st:Statement):Unit={
    val insertDatasets: String = "INSERT INTO datasets\n(\n    id, dataset_id, \"type\", \"name\", validation_config, extraction_config, \n    dedup_config, data_schema, denorm_config, router_config, dataset_config, \n    tags, data_version, status, created_by, updated_by, created_date, updated_date, \n    published_date, api_version, \"version\", sample_data, entry_topic\n)\nVALUES (\n    'new-york-taxi-data',\n    'new-york-taxi-data',\n    'event',\n    'New York Taxi Data',\n    '{\"validate\": true, \"mode\": \"Strict\"}',\n    '{\"is_batch_event\": true, \"extraction_key\": \"events\", \"dedup_config\": {\"drop_duplicates\": true, \"dedup_key\": \"id\", \"dedup_period\": 604800}}',\n    '{\"drop_duplicates\": true, \"dedup_key\": \"id\", \"dedup_period\": 604800}',\n    '{\"$schema\": \"https://json-schema.org/draft/2020-12/schema\", \"type\": \"object\", \"properties\": {\"tripID\": {\"key\": \"tripID\", \"type\": \"string\", \"arrival_format\": \"text\", \"data_type\": \"string\", \"isRequired\": false, \"resolved\": false}, \"VendorID\": {\"key\": \"VendorID\", \"type\": \"string\", \"arrival_format\": \"text\", \"data_type\": \"string\", \"isRequired\": false, \"resolved\": true}, \"tpep_pickup_datetime\": {\"key\": \"tpep_pickup_datetime\", \"type\": \"string\", \"arrival_format\": \"text\", \"data_type\": \"date-time\", \"isRequired\": false, \"resolved\": false}, \"tpep_dropoff_datetime\": {\"key\": \"tpep_dropoff_datetime\", \"type\": \"string\", \"arrival_format\": \"text\", \"data_type\": \"date-time\", \"isRequired\": false, \"resolved\": false}, \"passenger_count\": {\"key\": \"passenger_count\", \"type\": \"string\", \"arrival_format\": \"text\", \"data_type\": \"string\", \"isRequired\": false, \"resolved\": true}, \"trip_distance\": {\"key\": \"trip_distance\", \"type\": \"string\", \"arrival_format\": \"text\", \"data_type\": \"string\", \"isRequired\": false, \"resolved\": true}, \"RatecodeID\": {\"key\": \"RatecodeID\", \"type\": \"string\", \"arrival_format\": \"text\", \"data_type\": \"string\", \"isRequired\": false, \"resolved\": true}, \"store_and_fwd_flag\": {\"key\": \"store_and_fwd_flag\", \"type\": \"string\", \"arrival_format\": \"text\", \"data_type\": \"string\", \"isRequired\": false, \"resolved\": true}, \"PULocationID\": {\"key\": \"PULocationID\", \"type\": \"string\", \"arrival_format\": \"text\", \"data_type\": \"string\", \"isRequired\": false, \"resolved\": true}, \"DOLocationID\": {\"key\": \"DOLocationID\", \"type\": \"string\", \"arrival_format\": \"text\", \"data_type\": \"string\", \"isRequired\": false, \"resolved\": true}, \"payment_type\": {\"key\": \"payment_type\", \"type\": \"string\", \"arrival_format\": \"text\", \"data_type\": \"string\", \"isRequired\": false, \"resolved\": true}, \"primary_passenger\": {\"key\": \"primary_passenger\", \"type\": \"object\", \"arrival_format\": \"object\", \"data_type\": \"object\", \"isRequired\": false, \"resolved\": true, \"properties\": {\"email\": {\"key\": \"email\", \"type\": \"string\", \"arrival_format\": \"text\", \"data_type\": \"string\", \"isRequired\": false, \"resolved\": true}, \"mobile\": {\"key\": \"mobile\", \"type\": \"string\", \"arrival_format\": \"text\", \"data_type\": \"string\", \"isRequired\": false, \"resolved\": true}}, \"additionalProperties\": false}, \"fare_details\": {\"key\": \"fare_details\", \"type\": \"object\", \"arrival_format\": \"object\", \"data_type\": \"object\", \"isRequired\": false, \"resolved\": true, \"properties\": {\"fare_amount\": {\"key\": \"fare_amount\", \"type\": \"string\", \"arrival_format\": \"text\", \"data_type\": \"string\", \"isRequired\": false, \"resolved\": true}, \"extra\": {\"key\": \"extra\", \"type\": \"string\", \"arrival_format\": \"text\", \"data_type\": \"string\", \"isRequired\": false, \"resolved\": true}, \"mta_tax\": {\"key\": \"mta_tax\", \"type\": \"string\", \"arrival_format\": \"text\", \"data_type\": \"string\", \"isRequired\": false, \"resolved\": true}, \"tip_amount\": {\"key\": \"tip_amount\", \"type\": \"string\", \"arrival_format\": \"text\", \"data_type\": \"string\", \"isRequired\": false, \"resolved\": true}, \"tolls_amount\": {\"key\": \"tolls_amount\", \"type\": \"string\", \"arrival_format\": \"text\", \"data_type\": \"string\", \"isRequired\": false, \"resolved\": true}, \"improvement_surcharge\": {\"key\": \"improvement_surcharge\", \"type\": \"string\", \"arrival_format\": \"text\", \"data_type\": \"string\", \"isRequired\": false, \"resolved\": true}, \"total_amount\": {\"key\": \"total_amount\", \"type\": \"string\", \"arrival_format\": \"text\", \"data_type\": \"string\", \"isRequired\": false, \"resolved\": true}, \"congestion_surcharge\": {\"key\": \"congestion_surcharge\", \"type\": \"string\", \"arrival_format\": \"text\", \"data_type\": \"string\", \"isRequired\": false, \"resolved\": true}}, \"additionalProperties\": false}}, \"additionalProperties\": false}',\n    '{\"redis_db_host\": \"redis-denorm-headless.redis.svc.cluster.local\", \"redis_db_port\": 6379, \"denorm_fields\": []}',\n    '{\"topic\": \"new-york-taxi-data\"}',\n    '{\"file_upload_path\": [\"api-service/user_uploads/sample_538125.json\"], \"indexing_config\": {\"olap_store_enabled\": true, \"lakehouse_enabled\": true, \"cache_enabled\": false}, \"keys_config\": {\"data_key\": \"\", \"partition_key\": \"\", \"timestamp_key\": \"obsrv_meta.syncts\"}, \"cache_config\": {\"redis_db_host\": \"redis-denorm-headless.redis.svc.cluster.local\", \"redis_db_port\": 6379, \"redis_db\": 0}}',\n    '{\"tag1\", \"tag2\"}',  -- Replace with valid tags or NULL\n    0,\n    'Live',\n    'SYSTEM',\n    'SYSTEM',\n    now(),\n    now(),\n    now(),\n    'v1',\n    1,\n    '{\"mergedEvent\": {\"tripID\": \"dcca0f14-d92d-4720-a533-b77ce159a309\", \"VendorID\": \"1\", \"tpep_pickup_datetime\": \"2023-10-17 17:57:46\", \"tpep_dropoff_datetime\": \"2023-10-17 18:08:43\", \"passenger_count\": \"4\", \"trip_distance\": \".90\", \"RatecodeID\": \"1\", \"store_and_fwd_flag\": \"N\", \"PULocationID\": \"161\", \"DOLocationID\": \"186\", \"payment_type\": \"1\", \"primary_passenger\": {\"email\": \"Kayden_Roob@hotmail.com\", \"mobile\": \"517.714.5595 x5944\"}, \"fare_details\": {\"fare_amount\": \"8\", \"extra\": \"0\", \"mta_tax\": \"0.5\", \"tip_amount\": \"2.2\", \"tolls_amount\": \"0\", \"improvement_surcharge\": \"0.3\", \"total_amount\": \"11\", \"congestion_surcharge\": \"\"}}}'::json,\n    'dev.ingest'\n);"
    val insertConnectorRegistry: String = "INSERT INTO connector_registry(\n    id, connector_id, name, type, category, version, description, technology, runtime, licence, owner, status, ui_spec, source_url, source, created_by, updated_by, created_date, updated_date, live_date\n) \nVALUES (\n    'postgres-connector-1.0.0', \n    'postgres-connector', \n    'postgresql', \n    'source', \n    'Database', \n    '1.0.0', \n    'The PostgreSQL Connector is used to move data from any Postgres Table to Obsrv', \n    'scala', \n    'spark', \n    'MIT', \n    'Sunbird', \n    'Live', \n    '{\"title\": \"PostgreSQL JDBC Connector Setup Instructions\", \"description\": \"Configure PostgreSQL JDBC Connector\", \"helptext\": \"Follow the below instructions to populate the required inputs needed for the connector correctly.\", \"type\": \"object\", \"properties\": {\"source_database_host\": {\"type\": \"string\", \"title\": \"Host\", \"pattern\": \"^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\\\\-]*[a-zA-Z0-9])\\\\.)*([A-Za-z0-9]|[A-Za-z0-9][a-zA-Z0-9\\\\-]*[a-zA-Z0-9])$\", \"description\": \"Enter database server hostname or IP (e.g., db.example.com)\", \"helptext\": \"<p><strong>Host:</strong> Enter the IP address or hostname of the database server. <em>Example:</em> <em>192.168.1.1</em> or <em>db.example.com</em>.</p> <p><strong>Recommendation:</strong> If a replica server is available, it is preferable to connect to the replica instead of the main server. This can help reduce load on the primary database.</p>\", \"uiIndex\": 1}, \"source_database_port\": {\"type\": \"number\", \"title\": \"Port\", \"minimum\": 1, \"maximum\": 65535, \"default\": 5432, \"description\": \"Enter port number (default: 5432)\", \"helptext\": \"<p><strong>Port:</strong> Enter the port number of the database server. Default PostgreSQL port is <em>5432</em>.</p>\", \"uiIndex\": 2}}}', \n    'jdbc-connector-1.0.0-distribution.tar_9ea519.gz', \n    '{\"source\": \"jdbc-connector-1.0.0\", \"main_class\": \"org.sunbird.obsrv.connector.JDBCConnector\", \"main_program\": \"jdbc-connector-1.0.0.jar\"}', \n    'SYSTEM', \n    'SYSTEM', \n    now(), \n    now(), \n    now()\n);"
    val insertConnectorInstance: String = "INSERT INTO connector_instances (\n    id,\n    dataset_id,\n    connector_id,\n    connector_config,\n    operations_config,\n    status,\n    connector_state,\n    connector_stats,\n    created_by,\n    updated_by,\n    created_date,\n    updated_date,\n    published_date\n) VALUES (\n    'nyt-psql.1', \n    'new-york-taxi-data', \n\t'postgres-connector-1.0.0',\n\t'OBnGkE1P206Q+IBNL5cPtnan0+M0r5enNyoormaNbW4P64onl3HH0RVK2AtpWc4QgnhjcuyENPYWYsqMxk7IX048JTSBRjC7UqibBrJ1LMM2RLAjxo7RHXEFfjD3zy2wjrinWLw2yYf3inG4yE0gM9eEgWxmhDESbMO63JsaeIIFiJVztAjdyzsX/lMWPD94uDT8Fwqa49ZBFvSP2JTJLF4h28vu9YNRgQya5MP1f+WsUp6+X3i7Fx+C2J5wLGSrK8T1F5R9S+AsZT2CVxSRJzLM4Nh7AI3ArQZszZ9Wq/IBhF5SpFyZ9vx1xo0vSNxBsb9EmidU9jp8QCMzTAwosoRmr38qbDJG7TdDhpgrLGaNKtPfBQpFJN3njvR5G1a7C6ypocWGJonT9U26MhZTN0e1udqP8oFXcVXUTIoE4+kCrl9vJYbRb3PWtOTP6JP6EtYYvSIg5Mzw+GgGYAUpXhpRiSrnq1R6+0POCggmYDA=', \n    '{}'::json,  \n    'Live', \n    '{}'::json,  \n    '{}'::json, \n    'SYSTEM', \n    'SYSTEM', \n    now(), \n    now(), \n    now()\n);"
    val insertSampleData: String ="INSERT INTO new_york_taxi_data (\"VendorID\",tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,\"RatecodeID\",store_and_fwd_flag,\"PULocationID\",\"DOLocationID\",payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,congestion_surcharge,tripid,primary_passenger__email,primary_passenger__mobile,fare_details__fare_amount,fare_details__extra,fare_details__mta_tax,fare_details__tip_amount,fare_details__tolls_amount,fare_details__improvement_surcharge,fare_details__total_amount,fare_details__congestion_surcharge) VALUES\n\t (1,'2023-10-20 15:31:03','2023-01-14 15:34:52',0,0.7,1,'N',246,90,2,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'82ba482f-21a4-4d3d-af19-ab628acbf004','Jasmin50@gmail.com','(944) 834-7944',4.5,0,0.5,0,0,0.3,5.3,''),\n\t (1,'2023-05-28 15:39:40','2023-12-30 15:54:00',1,2.3,1,'N',68,45,1,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'59b8160d-71a7-4fb0-8904-765c5e7c21ff','Rocio_Wolff@yahoo.com','280-300-4957 x648',11.5,0,0.5,2,0,0.3,14.75,''),\n\t (2,'2023-07-09 15:42:53','2023-03-06 15:51:08',1,3.3,1,'N',132,216,1,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'ce28d110-ef5d-499a-bacd-53d0e0eac2f6','Josefina22@gmail.com','619.865.2733 x732',11.0,0,0.5,2,0,0.3,14.16,''),\n\t (2,'2023-03-20 15:24:55','2023-07-21 15:58:41',5,12.25,1,'N',138,186,2,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'469b39eb-6d33-4108-a1e7-7ad8ecd7552c','Alyce_Kling@gmail.com','469.644.7590 x05242',36.0,0,0.5,0,5,0.3,42.56,''),\n\t (2,'2023-09-22 15:39:31','2023-05-21 16:06:11',1,3.96,1,'N',237,158,1,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'3052a52e-392e-48ac-a778-4f70388a7ccb','Josiane22@gmail.com','1-553-316-8266 x54684',19.0,0,0.5,1,0,0.3,20.8,''),\n\t (2,'2023-06-11 15:13:34','2023-04-18 15:18:26',1,0.58,1,'N',236,75,2,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'405a633d-5501-4ab2-8017-ed5fbbf1baea','Candida.Swift-Mohr2@yahoo.com','1-433-451-0056 x96569',5.0,0,0.5,0,0,0.3,5.8,''),\n\t (1,'2024-01-13 15:13:11','2023-01-24 15:41:38',4,4.0,1,'N',148,230,1,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'ef50a132-3222-47f5-9e96-a85cd37d4e9e','Vida.Langworth55@yahoo.com','949-808-6830 x53601',19.0,0,0.5,5,0,0.3,25.7,''),\n\t (1,'2024-01-08 15:45:56','2023-06-17 15:55:22',1,1.6,1,'N',230,246,1,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'b72d9f37-5710-4312-9480-625d938abe87','Santino32@gmail.com','(735) 504-9902 x3388',8.0,0,0.5,2,0,0.3,11.4,''),\n\t (2,'2023-12-01 14:59:28','2023-08-17 15:06:20',1,1.04,1,'N',142,48,2,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'7aab4298-0b86-4f18-8936-e072126c99df','Maeve49@yahoo.com','630-914-4680 x981',6.5,0,0.5,0,0,0.3,7.3,''),\n\t (2,'2023-12-19 15:10:53','2023-09-04 15:19:19',1,0.84,1,'N',48,161,2,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'af321498-b47d-4326-9960-540d953fea43','Eldon68@gmail.com','304.891.2004 x160',7.0,0,0.5,0,0,0.3,7.8,''),\n\t (2,'2023-11-01 15:20:46','2023-09-20 15:37:37',1,1.39,1,'N',161,100,2,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'a5712eb6-45df-4284-b68d-933f36430b70','Katelyn.Crona16@yahoo.com','776.678.2781 x382',11.0,0,0.5,0,0,0.3,11.8,''),\n\t (2,'2023-11-11 15:05:52','2023-05-07 15:18:29',1,1.61,1,'N',68,100,1,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'6bb73fb2-31df-46f8-92d1-75ba5fd59da7','Milford.Bashirian@hotmail.com','782.356.9362 x76985',10.0,0,0.5,2,0,0.3,12.96,''),\n\t (2,'2023-04-01 15:23:53','2023-08-19 15:32:45',1,0.6,1,'N',186,234,2,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'4ea0934f-ac2b-4e68-90ec-7fc3ff155e69','Arlene29@gmail.com','1-207-694-6171',7.0,0,0.5,0,0,0.3,7.8,''),\n\t (2,'2023-08-25 15:38:43','2023-02-16 16:02:56',1,9.34,1,'N',164,138,2,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'d493f0fc-b910-43a3-8a03-4d702d10f30f','Tiara_Hudson88@gmail.com','1-317-813-0452 x200',29.0,0,0.5,0,5,0.3,35.56,''),\n\t (2,'2023-08-07 15:10:33','2023-12-29 15:21:07',2,1.21,1,'N',137,113,1,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'ac1cd737-f229-4cf0-8274-acce45800451','Karianne.Hirthe65@yahoo.com','1-940-594-5101 x52985',8.5,0,0.5,1,0,0.3,11.16,''),\n\t (2,'2023-11-02 15:23:42','2023-03-04 15:36:38',2,1.73,1,'N',113,100,1,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'c556c02c-9914-4a9d-b384-42eba82a1aed','Ivory47@yahoo.com','1-416-306-4189 x69759',9.5,0,0.5,2,0,0.3,12.36,''),\n\t (2,'2023-03-01 15:54:51','2023-08-12 16:05:35',2,1.66,1,'N',113,231,2,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'abfdb8c6-b3b7-4c1e-8e12-d5f9f32099e8','Kareem_Grady@gmail.com','(775) 527-4338 x6511',9.0,0,0.5,0,0,0.3,9.8,''),\n\t (2,'2023-05-06 15:25:46','2023-06-09 15:37:52',1,2.02,1,'N',249,234,2,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'cf364f8e-9d5b-41b9-ac9d-1410a2bdec7f','Brielle.Mayert-Casper@gmail.com','778.892.9406 x1397',10.0,0,0.5,0,0,0.3,10.8,''),\n\t (2,'2023-05-28 15:41:10','2023-06-13 15:50:24',1,1.27,1,'N',234,144,1,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'783ebf92-5a0b-41e8-a893-41d61f93c268','Delmer80@gmail.com','840-354-4960 x352',8.0,0,0.5,1,0,0.3,10.56,''),\n\t (2,'2023-04-05 15:58:10','2023-09-19 15:59:36',1,0.28,1,'N',249,113,1,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'32c29c4f-a30e-49ec-82e6-f7eae16e2c49','Silas.Volkman@yahoo.com','(634) 725-5488 x7276',3.0,0,0.5,0,0,0.3,4.56,''),\n\t (1,'2023-11-06 15:05:46','2023-07-12 15:28:59',2,5.1,1,'N',236,246,1,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'cc97ae9a-d7f0-4ce7-b104-6c7e060f0267','Andreane_Lynch33@yahoo.com','941-897-5396 x3395',19.5,0,0.5,4,0,0.3,24.35,''),\n\t (1,'2023-04-02 15:44:52','2024-02-01 15:57:57',1,1.4,1,'N',249,79,1,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'da9268eb-9928-4bff-b4c2-51060da18797','Conner_Beatty@hotmail.com','272-767-7384 x2115',9.5,0,0.5,1,0,0.3,11.8,''),\n\t (2,'2023-07-16 15:02:26','2023-06-10 15:07:48',1,0.85,1,'N',234,68,1,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'5e971725-a9a9-4860-bcc6-1efa879b3e71','Hope.Adams4@gmail.com','449.349.5822 x492',5.5,0,0.5,1,0,0.3,7.56,''),\n\t (2,'2023-07-08 15:10:44','2023-11-27 15:13:00',1,0.49,1,'N',68,246,2,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'a7b1f9bb-70f3-419e-814d-5019fd23653c','Ted.Watsica94@gmail.com','1-553-364-8009 x5903',4.0,0,0.5,0,0,0.3,4.8,''),\n\t (2,'2023-11-25 15:23:08','2023-11-01 15:24:55',1,0.34,1,'N',50,48,2,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'2b1e5fae-8457-42ea-b686-570e9237ca54','Tate63@hotmail.com','(340) 981-9365 x3236',3.5,0,0.5,0,0,0.3,4.3,''),\n\t (2,'2023-03-18 15:25:53','2023-11-08 15:31:47',1,1.25,1,'N',48,43,2,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'73ba8a88-833d-4241-a861-3ccd0a4f4b5a','Prince_Schaefer-Weimann4@gmail.com','688-606-5511 x78369',6.5,0,0.5,0,0,0.3,7.3,''),\n\t (2,'2023-02-02 15:37:41','2023-10-01 15:47:45',1,1.85,1,'N',142,237,1,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'709fe293-276f-4ed1-a551-e330c97ca6f2','Don48@gmail.com','(641) 840-2273 x426',9.0,0,0.5,1,0,0.3,11.76,''),\n\t (2,'2024-01-07 15:50:01','2023-11-01 15:56:09',1,1.03,1,'N',141,262,1,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'5b3d9e14-edea-4f68-93d0-678746642075','Bertha_Mann@gmail.com','434.372.2037 x419',6.5,0,0.5,1,0,0.3,8.76,''),\n\t (2,'2023-04-26 15:08:33','2023-01-17 15:11:53',1,0.34,1,'N',236,236,1,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'d8e0b7b2-8ee3-4350-9c79-0bb4a12c5dc3','Domenico_Toy@hotmail.com','(457) 771-9688',4.0,0,0.5,0,0,0.3,5.52,''),\n\t (2,'2023-11-23 15:13:35','2024-02-03 15:14:17',1,0.21,1,'N',263,236,2,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'f9077d5c-0bfc-4416-99bc-2f4853492166','Eldora_Corkery@gmail.com','1-923-530-3967 x5085',3.0,0,0.5,0,0,0.3,3.8,'');"
    st.executeUpdate(insertDatasets)
    st.executeUpdate(insertConnectorRegistry)
    st.executeUpdate(insertConnectorInstance)
    st.executeUpdate(insertSampleData)
    val res = st.executeQuery("Select count(*) from new_york_taxi_data;")
    if(res.next())
      {
        println("Sample Data Count: "+res.getInt(1))
      }
  }

  def setConnectorInstance(st:Statement):Unit={
    val deleteRecord:String = "DELETE FROM connector_instances;"
    st.executeUpdate(deleteRecord)
    val insertConnectorInstance: String = "INSERT INTO connector_instances (\n    id,\n    dataset_id,\n    connector_id,\n    connector_config,\n    operations_config,\n    status,\n    connector_state,\n    connector_stats,\n    created_by,\n    updated_by,\n    created_date,\n    updated_date,\n    published_date\n) VALUES (\n    'nyt-psql.1', \n    'new-york-taxi-data', \n\t'postgres-connector-1.0.0',\n\t'OBnGkE1P206Q+IBNL5cPtnan0+M0r5enNyoormaNbW4P64onl3HH0RVK2AtpWc4QgnhjcuyENPYWYsqMxk7IX048JTSBRjC7UqibBrJ1LMM2RLAjxo7RHXEFfjD3zy2wjrinWLw2yYf3inG4yE0gM9eEgWxmhDESbMO63JsaeIIFiJVztAjdyzsX/lMWPD94uDT8Fwqa49ZBFvSP2JTJLF4h28vu9YNRgQya5MP1f+WsUp6+X3i7Fx+C2J5wLGSrK8T1F5R9S+AsZT2CVxSRJzLM4Nh7AI3ArQZszZ9Wq/IBhF5SpFyZ9vx1xo0vSNxBsb9EmidU9jp8QCMzTAwosoRmr38qbDJG7TdDhpgrLGaNKtPfBQpFJN3njvR5G1a7C6ypocWGJonT9U26MhZTN0e1udqP8oFXcVXUTIoE4+kCrl9vJYbRb3PWtOTP6JP6EtYYvSIg5Mzw+GgGYAUpXhpRiSrnq1R6+0POCggmYDA=', \n    '{}'::json,  \n    'Live', \n    '{}'::json,  \n    '{}'::json, \n    'SYSTEM', \n    'SYSTEM', \n    now(), \n    now(), \n    now()\n);"
    st.executeUpdate(insertConnectorInstance)
  }

   def setWrongPassword(st:Statement):Unit={
     val setWrongPassword: String = "UPDATE connector_instances" +
       " SET connector_config='OBnGkE1P206Q+IBNL5cPtnan0+M0r5enNyoormaNbW4P64onl3HH0RVK2AtpWc4QgnhjcuyENPYWYsqMxk7IX048JTSBRjC7UqibBrJ1LMM2RLAjxo7RHXEFfjD3zy2wjrinWLw2yYf3inG4yE0gM9eEgWxmhDESbMO63JsaeIIFiJVztAjdyzsX/lMWPD94uDT8Fwqa49ZBFvSP2JTJLF4h28vu9YNRgQya5MP1f+WsUp6+X3i7Fx+C2J5wLGSrK8T1F5R9S+AsZT2CVxSRJzLM4Nh7AI3ArQZszZ9Wq/IBhF5SpFyZ9vx1xo0vSNxBsb9EmidU9jp8QCMzTAwosoRmr38qbDJG7TdDhpgrLGaNKtPfBQpFJN3njvR5G1a7C6ypocWGJonT9U26MhZTN1KgM47tBjvdqsybA9k9SZOqVXmazCGjfAoDXMWZ6JjADr9MwFpWRGzPpuYGs2+P1RmCNJUjCLkbrn68W8SP8h4=' WHERE id='nyt-psql.1';"
     st.executeUpdate(setWrongPassword);
   }
}

