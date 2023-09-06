package main

import (
	"log"
	"net/http"
	"time"
    "crypto/tls"
	"github.com/IBM/sarama"
	"github.com/linkedin/goavro/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/push"
)

var (
	kafkaBroker   = "pkc-6ojv2.us-west4.gcp.confluent.cloud:9092"
	kafkaTopic    = "final_latency_data"
	schemaRegistryURL = "https://psrc-4nyjd.us-central1.gcp.confluent.cloud"
	consumerGroup = "my-consumer-group"
	schemaSubject = "KsqlDataSourceSchema"
)

func main() {
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
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Version = sarama.V2_4_0_0 // Set the appropriate Kafka version

	// Configure SASL and TLS for secure connection
	config.Net.SASL.Enable = true
	config.Net.SASL.User = "72X53X5NBUPSITYT"
	config.Net.SASL.Password = "dYKiX5YD+ON4cA8tZys1d51Bhf62WHqGbpO7kovD1EI84XXNXUjquLAsir3NRtKs"
	config.Net.SASL.Mechanism = sarama.SASLMechanism("PLAIN")
	config.Net.SASL.Handshake = true
	config.Net.TLS.Enable = true
	config.Net.TLS.Config = &tls.Config{
		InsecureSkipVerify: true, // Skip certificate verification for simplicity, but it's not recommended in production.
	}

	consumer, err := sarama.NewConsumer([]string{kafkaBroker}, config)
	if err != nil {
		log.Fatal("Kafka consumer error:", err)
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(kafkaTopic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatal("Kafka partition consumer error:", err)
	}
	defer partitionConsumer.Close()

	// Set up Prometheus Push Gateway client
	pusher := push.New("http://localhost:9091", "kafka_connect_pipeline_e2e_latency")

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
			case msg := <-partitionConsumer.Messages():
				// Decode Avro message
				native, _, err := codec.NativeFromBinary(msg.Value)
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

				// Push the metric to Prometheus Push Gateway
				if err := pusher.Collector(e2eLatencyGauge).Push(); err != nil {
					log.Println("Prometheus Push Gateway error:", err)
				}

				log.Printf("Pushed metric: CORRELATION_ID=%s, CONNECT_PIPELINE_ID=%s, TS=%s, E2E_LATENCY=%.2f\n",
					correlationID, connectPipelineID, ts, e2eLatency)

			case err := <-partitionConsumer.Errors():
				log.Println("Kafka consumer error:", err)
			}
		}
	}()

	// Expose Prometheus metrics for scraping on port 9091
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		http.ListenAndServe(":9091", nil)
	}()

	// Keep the program running
	select {}
}
