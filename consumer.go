package main

import (
	"log"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/linkedin/goavro/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
)

func main() {
	// Load configuration from environment variables
	kafkaBroker := os.Getenv("KAFKA_BROKER")
	kafkaTopic := os.Getenv("KAFKA_TOPIC")
	//schemaRegistryUserInfo := os.Getenv("SCHEMA_REGISTRY_USER_INFO")
	//schemaRegistryURL := os.Getenv("SCHEMA_REGISTRY_URL")
	saslUsername := os.Getenv("SASL_USERNAME")
	saslPassword := os.Getenv("SASL_PASSWORD")
	promPushGatewayURL := os.Getenv("PROM_PUSHGATEWAY_URL")
	jobName := os.Getenv("JOB_NAME")

	// Set up Avro schema decoder
	codec, err := goavro.NewCodec(`
	{
		"type": "record",
		"name": "KsqlDataSourceSchema",
		"namespace": "io.confluent.ksql.avro_schemas",
		"fields": [
			{
				"name": "TS",
				"type": ["null", "long"]
			},
			{
				"name": "CORRELATION_ID",
				"type": ["null", "string"]
			},
			{
				"name": "CONNECT_PIPELINE_ID",
				"type": ["null", "string"]
			},
			{
				"name": "E2E_LATENCY",
				"type": ["null", "long"]
			}
		]
	}
`)
	if err != nil {
		log.Fatal("Avro schema error:", err)
	}

	// Set up Kafka consumer config
	config := kafka.ConfigMap{
		"bootstrap.servers": kafkaBroker,
		"group.id":          "my-consumer-group",
		"auto.offset.reset": "earliest",
		"sasl.mechanisms":   "PLAIN",
		"sasl.username":     saslUsername,
		"sasl.password":     saslPassword,
		"security.protocol": "sasl_ssl",
	}

	consumer, err := kafka.NewConsumer(&config)
	if err != nil {
		log.Fatal("Kafka consumer error:", err)
	}
	defer consumer.Close()

	err = consumer.SubscribeTopics([]string{kafkaTopic}, nil)
	if err != nil {
		log.Fatal("Kafka subscription error:", err)
	}

	// Set up Prometheus Push Gateway client
	pushURL := promPushGatewayURL + "/metrics/job/" + jobName

	pusher := push.New(pushURL, jobName)

	// Define Prometheus gauge metric for E2E_LATENCY
	e2eLatencyGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_connect_pipeline_e2e_latency",
			Help: "Kafka Connect Pipeline E2E Latency",
		},
		[]string{"CORRELATION_ID", "CONNECT_PIPELINE_ID"},
	)

	// Register the metric with Prometheus
	prometheus.MustRegister(e2eLatencyGauge)

	// Start a goroutine to consume Kafka messages and push metrics
	go func() {
		for {
			select {
			case ev := <-consumer.Events():
				switch e := ev.(type) {
				case *kafka.Message:
					// Decode Avro message
					native, _, err := codec.NativeFromBinary(e.Value)
					if err != nil {
						log.Println("Avro decoding error:", err)
						continue
					}

					record := native.(map[string]interface{})
					ts := time.Unix(0, record["TS"].(int64))
					correlationID := record["CORRELATION_ID"].(string)
					connectPipelineID := record["CONNECT_PIPELINE_ID"].(string)
					e2eLatency := float64(record["E2E_LATENCY"].(int64))

					// Update Prometheus metric
					e2eLatencyGauge.WithLabelValues(correlationID, connectPipelineID).Set(e2eLatency)

					log.Printf("Pushing metric: CORRELATION_ID=%s, CONNECT_PIPELINE_ID=%s, TS=%s, E2E_LATENCY=%.2f\n",
						correlationID, connectPipelineID, ts, e2eLatency)

				case kafka.Error:
					log.Println("Kafka consumer error:", e)
				}
			}
		}
	}()

	// Push metrics to Prometheus Push Gateway (moved outside the loop)
	if err := pusher.Collector(e2eLatencyGauge).Push(); err != nil {
		log.Println("Prometheus Push Gateway error:", err)
	}
}
