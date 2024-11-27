nohup java -Xmx512M -Xms512M -server \
    -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 \
    -XX:+ExplicitGCInvokesConcurrent -XX:MaxInlineLevel=15 -Djava.awt.headless=true \
    -Xlog:gc*:file=/var/log/kafka/zookeeper-gc.log:time,tags:filecount=10,filesize=100M \
    -Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.authenticate=false \
    -Dcom.sun.management.jmxremote.ssl=false -Dkafka.logs.dir=/var/log/kafka \
    -Dlog4j.configuration=file:/etc/kafka/log4j.properties \
    -cp /usr/bin/../share/java/kafka/*:/usr/bin/../share/java/confluent-telemetry/* \
    -Dsun.security.krb5.debug=true org.apache.zookeeper.server.quorum.QuorumPeerMain \
    /etc/kafka/zookeeper.properties &

sleep 5

nohup java -Xmx1G -Xms1G -server \
    -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 \
    -XX:+ExplicitGCInvokesConcurrent -XX:MaxInlineLevel=15 -Djava.awt.headless=true \
    -Xlog:gc*:file=/var/log/kafka/kafkaServer-gc.log:time,tags:filecount=10,filesize=100M \
    -Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.authenticate=false \
    -Dcom.sun.management.jmxremote.ssl=false -Dkafka.logs.dir=/var/log/kafka \
    -Dlog4j.configuration=file:/etc/kafka/log4j.properties \
    -cp /usr/bin/../share/java/kafka/*:/usr/bin/../share/java/confluent-telemetry/* \
    -Dsun.security.krb5.debug=true -Djava.security.auth.login.config=/etc/kafka/kafka_server_jaas.conf \
    -Djava.security.krb5.conf=/etc/krb5.conf kafka.Kafka /etc/kafka/kafka.properties &

sleep 5

tail -f /var/log/kafka/server.log