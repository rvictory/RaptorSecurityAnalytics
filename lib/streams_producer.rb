# Streams Producer - Simplified Producer API for MapR-Streams
# This is proof of concept code, not intended for Production yet!

include Java
Dir["/opt/mapr/lib/\*.jar"].each { |jar| require jar }
require 'sinatra'

java_import com.google.common.io.Resources
java_import org.apache.kafka.clients.producer.KafkaProducer
java_import org.apache.kafka.clients.producer.ProducerRecord

java_import java.io.IOException
java_import java.io.InputStream
java_import java.util.Properties


class StreamsProducer

  # Intiliazes a Streams Producer, note that there aren't any special parameters here
  def initialize
    properties = {
        'batch.size'=>'16384',
        'key.serializer'=>'org.apache.kafka.common.serialization.StringSerializer',
        'value.serializer'=>'org.apache.kafka.common.serialization.StringSerializer',
        'block.on.buffer.full'=>'true'
    }
    @producer = KafkaProducer.new(properties)
  end

  # Publishes a message to the specified stream and topic
  def publish (stream, topic, message)
    raise Exception.new("Producer has been closed") if @producer.nil?
    if stream[0] == '/'
      stream_and_topic = stream + ":" + topic
    else
      stream_and_topic = "/" + stream + ":" + topic
    end
    @producer.send(ProducerRecord.new(stream_and_topic, message))
    @producer.flush
  end

  # Closes the current connection. This will end the usefulness of this StreamsProducer instance
  def close
    @producer.close
    @producer = nil
  end

end