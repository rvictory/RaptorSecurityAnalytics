# Raptor Security Analytics Platform: Realtime Security Event Processing

This repository holds some of my sample code until I can start getting this project more organized. It is designed to run on MapR Streams and MapR-DB.

## Running on Marathon/Mesos
I am running these models/servers on Marathon. Here are the steps I'm using:
### For the Models (not the HTTP Server):
1. Download the JRuby standalone JAR (http://jruby.org/download)
2. Place the JAR in a shared Filesystem (MapR-FS)
3. Invoke the jobs in Marathon using the following command line: `java -jar /mapr/cluster.raptor.beer/frameworks/jruby.jar /mapr/cluster.raptor.beer/models/dns_events/dns_consumer.rb`

### For the HTTP Server
You will need to bundle up the Sinatra gem (and related gems) into a JAR file to provide to the standalone JRuby. To do this, follow these steps first:
1. Create a new gems directory (`mkdir gems`)
2. Execute the JRuby Gem installer to install sinatra into the new gems directory (change the path to JRuby): `java -jar /mapr/cluster.raptor.beer/frameworks/jruby.jar -S gem install -i ./gems sinatra --no-rdoc --no-ri`
3. Bundle that directory into a JAR: `jar cf gems.jar -C gems .`
4. Move that JAR into a shared Filesystem (MapR-FS)
5. Execute the Sinatra server using the following command line: `java -jar /mapr/cluster.raptor.beer/frameworks/jruby.jar -r /mapr/cluster.raptor.beer/frameworks/gems.jar /mapr/cluster.raptor.beer/apps/streams_rest_server/server.rb
`
