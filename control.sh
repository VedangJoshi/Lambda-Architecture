#!/bin/sh

Sys="${1}"
Option="${2}"

echo "\n\t *** Control script ***   \n"

case ${Sys} in 
	-z) echo "Zookeeper ${Option}"
		./zookeeper/bin/zkServer.sh ${Option}
	;;
	-k) case ${Option} in
			-B) 
				echo "Kafka starting..."	
				./kafka/bin/kafka-server-start.sh ./kafka/config/server.properties &				
				sleep 4
			;;
			-E) 
				echo "Kafka stop..."	
				./kafka/bin/kafka-server-stop.sh ./kafka/config/server.properties &
				sleep 4
			;;
		esac	
	;;	
	-p) echo "Producer running..."
		./kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic ${Option}
	;;
	-c) echo "Consumer running..."
		./kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic ${Option}
	;;		
	-l) echo "Topics :"
		./kafka/bin/kafka-topics.sh --list --zookeeper localhost:2181
	;;	
esac
