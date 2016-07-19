# Streams REST Server that runs on Sinatra
# Accepts HTTP POSTs with data to be put on the specified stream/topic
# TODO: need to update this to use the streams_prodcuer.rb API in /lib

include Java
Dir["/opt/mapr/lib/\*.jar"].each { |jar| require jar }
require 'sinatra'

java_import com.google.common.io.Resources
java_import org.apache.kafka.clients.producer.KafkaProducer
java_import org.apache.kafka.clients.producer.ProducerRecord

java_import java.io.IOException
java_import java.io.InputStream
java_import java.util.Properties


configure do
  set :bind, '0.0.0.0'
  env_port = ENV['PORT0']
  if env_port
    set :port, env_port.to_i
  else
    set :port, 4567
  end
  properties = {
      'batch.size'=>'16384',
      'key.serializer'=>'org.apache.kafka.common.serialization.StringSerializer',
      'value.serializer'=>'org.apache.kafka.common.serialization.StringSerializer',
      'block.on.buffer.full'=>'true'
  }
  @@producer = KafkaProducer.new(properties)
end


TOPIC_FAST_MESSAGES = "/sample-stream:fast-messages"


post '/send/:destination' do
  puts "Sending to #{params[:destination]}"
  data = request.body.read.to_s
  puts "Data: #{data}"
  @@producer.send(ProducerRecord.new("/" + params[:destination], data))
  @@producer.flush()
  ""
end

get '/health' do
  "All Good Here"
end