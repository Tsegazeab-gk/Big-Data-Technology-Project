TOPIC="topic-tweets"
PROJECT_PATH=/home/$USER/Desktop/Project
while getopts t: flag
do
    case "${flag}" in
        t) TOPIC=${OPTARG};;
    esac
done

gnome-terminal -t KAFKA-BROKER -x sh -c "kafka-server-start.sh $KAFKA_HOME/config/server.properties;bash"

sleep 6 & previous_pid=$!
wait $previous_pid

#Topics
kafka-topics.sh --list --bootstrap-server localhost:9092

#Create new topic 
#kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic $TOPIC 


gnome-terminal -t KAFKA-PRODUCER -x sh -c "kafka-console-producer.sh --broker-list localhost:9092 --topic $TOPIC;bash"

gnome-terminal -t KAFKA-CONSUMER -x sh -c "kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic $TOPIC;bash"
