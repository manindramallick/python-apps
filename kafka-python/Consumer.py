import json

from kafka import KafkaConsumer

kafkaBrokers = ['vlmazebpoc2ap02.fisdev.local:9094']
caRootLocation = 'C:/Users/e5617765/pythonDemo/FISEventBroker-ClientTruststore.pem'
certLocation = 'C:/Users/e5617765/pythonDemo/svc-acbs-api-dev.pem'
keyLocation = 'C:/Users/e5617765/pythonDemo/svc-acbs-api-dev.key'
topic = 'fis.customer.profile-name-change.acbs-customer.dev'
username = 'svc-acbs-api-dev'
password = '*******'
security_protocol = "SSL"
group_id = 'fis.customer.test'
consumer = KafkaConsumer(
                         bootstrap_servers=kafkaBrokers,
                         security_protocol='SSL',
                         ssl_check_hostname=True,
                         ssl_cafile=caRootLocation,
                         ssl_certfile=certLocation,
                         ssl_keyfile=certLocation,
                         ssl_password=password,
                         key_deserializer=lambda v: json.loads(v.decode('utf-8')),
                         value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                         group_id=group_id,
                         auto_offset_reset='earliest'
                         )
consumer.subscribe(topic)
for msg in consumer:
    print(msg.value)
