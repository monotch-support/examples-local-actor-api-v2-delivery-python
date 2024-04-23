import logging
import threading
import requests
import time
import sys
import os
import json
from proton.handlers import MessagingHandler
from proton.reactor import Container
from proton import SSLDomain, Message

# Configuration by environment variables
ACTOR_API_HOST=os.environ.get("ACTOR_API_HOST", "hostname_of_the_actor_api")
ACTOR_API_PORT=os.environ.get("ACTOR_API_PORT", "port_of_the_actor_api")
ACTOR_API_DELIVERY_SELECTOR=os.environ.get("ACTOR_API_DELIVERY_SELECTOR", "selector_of_the_delivery")
ACTOR_COMMON_NAME=os.environ.get("ACTOR_COMMON_NAME", "cn_of_the_actor_client_certificate")
ACTOR_CERTIFICATE_CHAIN_AND_KEY_PEM=os.environ.get("ACTOR_CERTIFICATE_CHAIN_AND_KEY_PEM", "pem_with_x509_certificate_chain_and_private_key")
CA_CERTIFICATE_PEM=os.environ.get("CA_CERTIFICATE_PEM", "pem_with_x509_certificate")
MESSAGE_APPLICATION_PROPERTIES_JSON=os.environ.get("MESSAGE_APPLICATION_PROPERTIES_JSON", "message_application_properties_json")


# ======== ACTOR API FUNCTIONS ========
def api_url(endpoint):
    return "https://%s:%s/%s/%s" % (ACTOR_API_HOST, ACTOR_API_PORT, ACTOR_COMMON_NAME, endpoint) 

def api_get(endpoint):
    return requests.get(api_url(endpoint), verify=CA_CERTIFICATE_PEM, cert=ACTOR_CERTIFICATE_CHAIN_AND_KEY_PEM)

def api_post(endpoint, json_data):
    return requests.post(api_url(endpoint), None, json_data, verify=CA_CERTIFICATE_PEM, cert=ACTOR_CERTIFICATE_CHAIN_AND_KEY_PEM)

def api_delete(endpoint):
    return requests.delete(api_url(endpoint), verify=CA_CERTIFICATE_PEM, cert=ACTOR_CERTIFICATE_CHAIN_AND_KEY_PEM)

def api_get_delivery(id):
    return api_get("deliveries/%s" % id)

def api_delete_delivery(id):
    return api_delete("deliveries/%s" % id)

def api_create_delivery():
    json_data = {
		"selector": ACTOR_API_DELIVERY_SELECTOR
    }
    return api_post("deliveries", json_data)	

# ======== AMQP 1.0 CLIENT ========
def amqp_create_ssl_config():
	ssl_config = SSLDomain(SSLDomain.MODE_CLIENT)
	ssl_config.set_peer_authentication(SSLDomain.ANONYMOUS_PEER)
	ssl_config.set_credentials(cert_file=ACTOR_CERTIFICATE_CHAIN_AND_KEY_PEM, key_file=ACTOR_CERTIFICATE_CHAIN_AND_KEY_PEM, password=None)
	ssl_config.set_trusted_ca_db(CA_CERTIFICATE_PEM)
	return ssl_config

class Sender(MessagingHandler):
	def __init__(self, endpoint):
		super(Sender, self).__init__()
		self.__endpoint = endpoint
	
	def on_start(self, event):
		logging.debug("Container reactor started")
		container = event.container
		endpoint = self.__endpoint

		# Step 1: connect
		ssl_config = amqp_create_ssl_config()
		amqp_url = "amqps://%s:%s" % (endpoint["host"], endpoint["port"])
		connection = container.connect(amqp_url, ssl_domain = ssl_config, reconnect = False, heartbeat = 5)

		# Step 2: create a sending link using the target address of the endpoint
		self.sender_link = container.create_sender(connection, endpoint["target"])
		self.sending = True
		
	def send_message(self):
		self.sending = False
		message = Message(body="Hello World!", properties=json.loads(MESSAGE_APPLICATION_PROPERTIES_JSON))
		logging.info("Sending message %s", message)
		self.sender_link.send(message)

	def on_sendable(self, event):
		if self.sending:
			self.send_message()

	def on_settled(self, event):
		logging.info("Message %s settled", event.delivery)
		self.sending = True

	def on_reactor_quiesced(self, event):
		if self.sending:
			self.send_message()

def amqp_connect_and_publish(endpoint):
	sender = Sender(endpoint)
	container = Container(sender)
	thread = threading.Thread(name = "AMQPClient", target = container.run, daemon = True)
	thread.start()
	while thread.is_alive():
		time.sleep(1)

# ======== CREATE AND PUBLISH INTO A DELIVERY ========
def log_json(message, json_dict):
	logging.info("%s: %s" % (message, json.dumps(json_dict, indent=2)))

def create_and_publish():
	try:
		# Step 1: create a delivery using the actor API
		delivery_create_response = api_create_delivery()
		delivery_create_response_json = delivery_create_response.json();
		log_json("Delivery create response", delivery_create_response_json)

		if delivery_create_response.ok:
			# Step 2: get the delivery status
			delivery_id = delivery_create_response_json["id"]
			delivery_status_response = api_get_delivery(delivery_id)
			delivery_status_response_json = delivery_status_response.json()
			log_json("Delivery %s status response" % delivery_id, delivery_status_response_json)
			delivery_status = delivery_status_response_json["status"]

			# Step 3: while the delivery status is "REQUESTED", keep getting the status
			while delivery_status == "REQUESTED":
				time.sleep(2)
				delivery_status_response = api_get_delivery(delivery_id)
				delivery_status_response_json = delivery_status_response.json()
				delivery_status = delivery_status_response_json["status"]

			# Step 4a: if the status is "CREATED", connect to the endpoint and start the AMQP publisher
			log_json("Delivery %s status response" % delivery_id, delivery_status_response_json)
			if delivery_status == "CREATED":
				# Step 5: get the endpoint information from the status response
				# NOTE to keep things simple, this code assumes that this response contains exactly one endpoint!
				endpoint = delivery_status_response_json["endpoints"][0]
				
				# Step 6: use the endpoint with the AMQP 1.0 client
				logging.info("Using to endpoint %s" % endpoint)
				amqp_connect_and_publish(endpoint)
			
			# Step 4b: if the status is not "CREATED" warn log and do nothing
			else:
				logging.warning("Unable to use delivery %s" % delivery_id)

	except Exception as e:
		logging.warning("An exception occurred while running create_and_publish: %s" % e)

# ======== STARTUP AND RUN LOOP ========
def dump_config():
	logging.info("ACTOR_API_HOST: '%s'" % ACTOR_API_HOST);
	logging.info("ACTOR_API_PORT: '%s'" % ACTOR_API_PORT);
	logging.info("ACTOR_API_DELIVERY_SELECTOR: '%s'" % ACTOR_API_DELIVERY_SELECTOR);
	logging.info("ACTOR_COMMON_NAME: '%s'" % ACTOR_COMMON_NAME)
	logging.info("ACTOR_CERTIFICATE_CHAIN_AND_KEY_PEM: '%s'" % ACTOR_CERTIFICATE_CHAIN_AND_KEY_PEM)
	logging.info("CA_CERTIFICATE_PEM: '%s'" % CA_CERTIFICATE_PEM)
	logging.info("MESSAGE_APPLICATION_PROPERTIES_JSON: '%s'" % MESSAGE_APPLICATION_PROPERTIES_JSON)
	

def configure_logging():
	logging.basicConfig(format = "%(asctime)s %(levelname)s %(message)s", level = logging.DEBUG)
	logging.getLogger("proton").setLevel(logging.INFO)

def main():
	configure_logging()
	logging.info("Starting application")
	dump_config()
	try:
		create_and_publish()
	except KeyboardInterrupt:
		logging.info("Application stopped")

main()
