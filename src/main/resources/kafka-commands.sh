
bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic events

bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic events-upper

bin\windows\kafka-topics.bat --zookeeper localhost:2181 --list

bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --all-groups --describe

bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic events --from-beginning --formatter kafka.tools.DefaultMessageFormatter --property print.key=true --property print.value=true --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer

bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic events-upper --from-beginning --formatter kafka.tools.DefaultMessageFormatter --property print.key=true --property print.value=true --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer

bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic poc-app-poc-state-store-changelog --from-beginning --formatter kafka.tools.DefaultMessageFormatter --property print.key=true --property print.value=true --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer