(ns testkafka.producer
  (:import
    (java.util Properties)
    (org.apache.kafka.clients.producer Callback KafkaProducer ProducerConfig ProducerRecord)
    (org.apache.kafka.common.serialization StringSerializer)))



(defn return-properties
  []
  (doto
    (new Properties)
    (.setProperty ProducerConfig/BOOTSTRAP_SERVERS_CONFIG "127.0.0.1:9092")
    (.setProperty ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG (.getName StringSerializer))
    (.setProperty ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG (.getName StringSerializer))))



(defn send-message
  []
  (let [prop (return-properties)
        producer (new KafkaProducer prop)
        record (new ProducerRecord "ECOMERCE_NEW_ORDER" "77777777" "123456")]
    ;(println (. prop keySet))
    (.send producer record)))

(send-message)