import json

from kafka import KafkaProducer

in_put = input("Message for Kafka Producer :")  # Get the input
kafkaBrokers = ['vlmazebpoc2ap02.fisdev.local:9094']
caRootLocation = 'C:/Users/e5617765/pythonDemo/FISEventBroker-ClientTruststore.pem'
certLocation = 'C:/Users/e5617765/pythonDemo/svc-acbs-api-dev.pem'
keyLocation = 'C:/Users/e5617765/pythonDemo/svc-acbs-api-dev.key'
topic = 'fis.customer.profile-name-change.acbs-customer.dev'
username = 'svc-acbs-api-dev'
password = '*********'
security_protocol = "SSL"

producer = KafkaProducer(bootstrap_servers=kafkaBrokers,
                         security_protocol='SSL',
                         ssl_check_hostname=True,
                         ssl_cafile=caRootLocation,
                         ssl_certfile=certLocation,
                         ssl_keyfile=certLocation,
                         ssl_password=password,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                         key_serializer=lambda v: json.dumps(v).encode('utf-8')
                         )

producer.send(topic, in_put)
producer.flush()
