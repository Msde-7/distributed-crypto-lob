#!/bin/bash
# Start the Kafka broker (KRaft mode). Run in its own terminal window; Ctrl+C to stop.

export JAVA_HOME="C:\\Program Files\\Microsoft\\jdk-17.0.18.8-hotspot"
export KAFKA_HOME="C:\\tools\\kafka_2.13-3.9.0"
export PATH="/c/Program Files/Microsoft/jdk-17.0.18.8-hotspot/bin:$PATH"

"${KAFKA_HOME}/bin/windows/kafka-server-start.bat" "${KAFKA_HOME}/config/kraft/server.properties"
