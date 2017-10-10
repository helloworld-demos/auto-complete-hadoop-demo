# How?

1. setup docker network

   ```shell
   $ docker network create \
       --driver=bridge \
       --subnet=172.18.0.0/16 \
       --gateway=172.18.0.1 \
       hadoop
     
   # docker network ls
   # docker network inspect hadoop
   ```

2. setup mysql with `172.18.0.2/16` in docker `hadoop` netowrk

   ```shell
   $ docker run --name hadoop-mysql \
       -e MYSQL_ROOT_PASSWORD=password \
       -e MYSQL_DATABASE=auto-complete \
       -e MYSQL_USER=hadoop \
       -e MYSQL_PASSWORD=hadoop \
       -p 3306:3306 \
       --net=hadoop \
       --ip=172.18.0.2 \
       -d \
       mysql:5.7
     
   # docker inspect hadoop
   ```

3. `./prepare.sh`

   1. generate application jar
   2. move applicatoin jar, data seed files and mysql-connector jar to shared folder
   3. recreate database by maven flyway plugin
   4. delete hadoop containers

4. `~/dev/hadoop-cluster-docker/start-container.sh`

5. run application on hadoop master

   `(cd src/auto-complete-demo/ && ./demo.sh)`

6. `select * from auto_complete where starting_phrase like 'a%';`