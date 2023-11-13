# backend-container-spawner

Docker container spawner for multiagent ordering system backend. The app configuration is located in the config_handler folder, commands are received as .json messages via RabbitMQ.

For now, everything is designed to run on a single local machine, Redis and RabbitMQ must be running on the same machine on their respective default ports.

![architecture overview](https://github.com/testbedCIIRC/multiagent-container-spawner/blob/master/backend_schema.png)
