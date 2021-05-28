(ns testkafka.consumer
  (:import
    (org.apache.kafka.clients.consumer ConsumerConfig KafkaConsumer)
    (org.apache.kafka.common.serialization StringDeserializer)
    (java.util Properties)
    (java.time Duration)))


(defn return-properties
  []
  (doto
    (new Properties)
    (.setProperty ConsumerConfig/BOOTSTRAP_SERVERS_CONFIG "127.0.0.1:9092")
    (.setProperty ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG (.getName StringDeserializer))
    (.setProperty ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG (.getName StringDeserializer))
    (.setProperty ConsumerConfig/GROUP_ID_CONFIG "consumer-test")))

(defn print-message-details
  [record]
  (println "---------------------------------")
  (println "details")
  (println (str "key: " (.key record)))
  (println (str "value: " (.value record)))
  (println (str "partition: " (.partition record)))
  (println "---------------------------------"))

(defn listen-topic
  []
  (let [props (return-properties)
        consumer (new KafkaConsumer props)]
    (.subscribe consumer ["ECOMERCE_NEW_ORDER"])

    (loop [records nil]
      (println "Funcionando: " (type records))
      (doseq [record records]
        (print-message-details record))
      (recur (.poll consumer (Duration/ofMillis 1000))))))

(comment (listen-topic))
