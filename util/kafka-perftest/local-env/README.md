## Performance Test Environment

This project initialises a dockerized environment with a 3 broker kafka cluster, 2 connect nodes and some other utilities.

Before starting the Docker Compose environment, make sure to set the following two environment variables to the IP address of the Docker host. 

```
export DOCKER_HOST_IP=192.xx.xx.xx 
export PUBLIC_IP=192.xx.xx.xx 
```

And then start the environment using 

```
docker-compose up -d
```