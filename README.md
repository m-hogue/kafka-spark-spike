# Kafka-Spark spike
The purpose of this project is to prove out spark streaming, where a spark process consumes information published
to a kafka queue and does _something_ with it.

### Kafka
A simple producer that publishes a serialized <code>RandomObject</code> to a kafka topic.


### Spark
A simple spark job that consumes messages from the <code>random-topic</code> kafka topic and does something with it.
