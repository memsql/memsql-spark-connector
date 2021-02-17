## singlestore-spark-connector demo

This is Dockerfile which uses the upstream [Zeppelin Image](https://hub.docker.com/r/apache/zeppelin/) as it's base
and has two notebooks with examples of singlestore-spark-connector.

To run this docker with [MemSQL CIAB](https://hub.docker.com/r/memsql/cluster-in-a-box) follow the instructions

* Create a docker network to be able to connect zeppelin and memsql-ciab
```
docker network create zeppelin-ciab-network
```

* Pull memsql-ciab docker image
```
docker pull memsql/cluster-in-a-box:centos-7.3.2-a364d4b31f-3.0.0-1.9.3
```

* Run and start the SingleStore Cluster in a Box docker container

```
docker run -i --init \
--name singlestore-ciab-for-zeppelin \
-e LICENSE_KEY=[INPUT_YOUR_LICENSE_KEY] \
-e ROOT_PASSWORD="password" \
-p 3306:3306 -p 8080:8080 \
--net=zeppelin-ciab-network \
memsql/cluster-in-a-box:centos-7.3.2-a364d4b31f-3.0.0-1.9.3
```
```
docker start singlestore-ciab-for-zeppelin
```
> :note: in this step you can hit a port collision error
>
> ```
> docker: Error response from daemon: driver failed programming external connectivity on endpoint singlestore-ciab-for-zeppelin
> (38b0df3496f1ec83f120242a53a7023d8a0b74db67f5e487fb23641983c67a76):
> Bind for 0.0.0.0:8080 failed: port is already allocated.
> ERRO[0000] error waiting for container: context canceled
> ```
>
> If it happened then remove the container
>
>`docker rm singlestore-ciab-for-zeppelin`
>
> and run the first command with other ports `-p {new_port1}:3306 -p {new_port2}:8080`

* Build zeppelin docker image in `singlestore-spark-connector/demo` folder

```
docker build -t zeppelin .
```

* Run zeppelin docker container
```
docker run -d --init \
--name zeppelin \
-p 8082:8082 \
--net=zeppelin-ciab-network \
-v $PWD/notebook:/opt/zeppelin/notebook/Singlestore \
zeppelin
```

> :note: in this step you can hit a port collision error
>
> ```
> docker: Error response from daemon: driver failed programming external connectivity on endpoint zeppelin
> (38b0df3496f1ec83f120242a53a7023d8a0b74db67f5e487fb23641983c67a76):
> Bind for 0.0.0.0:8082 failed: port is already allocated.
> ERRO[0000] error waiting for container: context canceled
> ```
>
> If it happened then remove the container
>
>`docker rm zeppelin`
>
> and run this command with other port `-p {new_port}:8082`


* open [zeppelin](http://localhost:8082/next) in your browser and try
[scala](http://localhost:8082/next/#/notebook/2F8XQUKFG),
[pyspark](http://localhost:8082/next/#/notebook/2F6Y3APTX)
and [spark sql](http://localhost:8082/next/#/notebook/2F7PZ81H6) notebooks

For setting up more powerful SingleStore trial cluster use [Helios](https://www.singlestore.com/helios/)