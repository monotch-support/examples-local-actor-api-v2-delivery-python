# Introduction

The main.py Python 3 script is a working example for setting up a “Local Actor v2 Delivery” on an interchange supporting the “Local Actor API v2”. The script contains all the functions required for the API and AMQP connectivity. The script is not intended for production use.


# Prerequisites
  
 - Libraries Python 3
 - Python packages: requests 
 - Python-qpid-proton 


# Adjust this according your information

 - ACTOR_API_HOST=
 - ACTOR_API_PORT=
 - ACTOR_API_DELIVERY_SELECTOR= *Such as "messageType = 'denm'"*
 - ACTOR_COMMON_NAME= *your complete actor name*
 - ACTOR_CERTIFICATE_CHAIN_AND_KEY_PEM= *your complete crt chain*
 - CA_CERTIFICATE_PEM= *your full chain root.crt*
 - MESSAGE_APPLICATION_PROPERTIES_JSON= *the AMQP message application properties in JSON*


# Howto run

 1. Set the ENV variables 
 2. Execute script with python 


# Example 

In the example below the script is executed in a docker container based on a default Ubuntu Jammy image.

The script uses the following fake configuration values:

- API hostname: `my-interchange`
- API port: `443`
- Delivery selector: `messageType = 'DENM'`
- Actor common name: `actor.my-interchange`
- Actor certificate chain and key file: `chain_and_key.pem`
- Certificate Authority certificate file: `ca.pem`

Since the default Ubuntu Jammy image needs additional packages installed for proton to support SSL, it includes the installation of the `pck-config` and `libssl-dev` (see `No SSL Support in proton` in the `Common mistakes` chapter).

```
FROM ubuntu:jammy

# install prerequisites
RUN apt update
RUN apt install python3 -y
RUN apt install pip -y
RUN apt install pkg-config libssl-dev -y 
RUN pip install requests
RUN pip install python-qpid-proton

# set environment variables
ENV ACTOR_API_HOST="my-interchange"
ENV ACTOR_API_PORT="443"
ENV ACTOR_API_DELIVERY_SELECTOR="publicationId = 'XX99999:DENM_01'"
ENV ACTOR_COMMON_NAME="actor.my-interchange"
ENV ACTOR_CERTIFICATE_CHAIN_AND_KEY_PEM="chain_and_key.pem"
ENV CA_CERTIFICATE_PEM="ca.pem"
ENV MESSAGE_APPLICATION_PROPERTIES_JSON='{"messageType": "DENM", "publicationId": "XX99999:DENM_01", "publisherId": "XX99999", "protocolVersion": "DENM:1.3.1", "quadTree": ",0,", "originatingCountry": "XX"}'

# add certificates and script
ADD chain_and_key.pem .
ADD ca.pem .
ADD main.py .

# execute script 
CMD python3 main.py
```


# Common mistakes

## No SSL support in proton

Sometimes the required depenencies are missing during the installation of the `python-qpid-proton` package causing the proton module to miss SSL support. This can be checked by running the following command: 
```
python3 -c "import proton; print('%s' % 'SSL present' if proton.SSL.present() else 'SSL NOT AVAILBLE')"`
```


## Certificate related

 - Not sending a full certificate chain (must include the client certificate, all intermediate certificates and the root certificate)
 - Not configuring the custom truststore (must include the root certificate)
