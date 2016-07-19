# Streams Consumer: a barebones MapR-Streams consumer for JRuby
# This is still proof of concept code, it /might/ be ready for production, probably not
# I will be improving the interface/configuration options in the future and adding an event based system (no polling)

include Java
Dir["/opt/mapr/lib/\*.jar"].each { |jar| require jar }

java_import com.google.common.io.Resources
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer

import java.io.IOException
import java.io.InputStream
import java.util.Arrays
import java.util.Properties

class StreamsConsumer

  # Creates a new consumer with the specified group ID and topic list
  def initialize(group_id, topics)
    conf = {
        'group.id' => group_id,
        'enable.auto.commit'=>'true',
        'key.deserializer' => 'org.apache.kafka.common.serialization.StringDeserializer',
        'value.deserializer'=>'org.apache.kafka.common.serialization.StringDeserializer',
        'fetch.min.bytes'=> '50000',
        'max.partition.fetch.bytes'=>'2097152',
        'auto.offset.reset'=>'earliest'
    }
    @topics = topics
    @consumer = KafkaConsumer.new(conf)
    @consumer.subscribe([topics].flatten)
  end

  # Polls for new messages with the specified timeout. Note, this returns a list of messages, not just one
  def poll(timeout = 200)
    @consumer.poll(timeout)
  end

end