# DNS History By Domain Name: Reads Bro DNS events from the MapR Stream and updates aggregate information in MapR-DB
# TODO: Create an alert stream and alert on new names
# TODO: Change to MapR-DB Mutations instead of updating the whole document (concurrency issues)
# TODO: Write Stats to a statistics stream
# TODO: General cleanup

include Java
require_relative '../lib/streams_consumer'
require_relative '../lib/maprdb'
require 'json'

consumer = StreamsConsumer.new('dns_by_domain_name', ["/bro-events:dns"])
table = MapR_DB.new("domain_name_history")

count = 0

loop do
  records = consumer.poll(200)
  if records.count() == 0
    # Do nothing
  else
    records.each do |record|
      count += 1
      if count >= 100
        print "#"
        count = 0
      end
#      puts "Received Message on topic #{record.topic}:\n\t#{record.value}"
      # We need to look up if this domain name already exists in MapR-DB
      begin
        json_record = JSON.parse(record.value)
      rescue
        # Invalid JSON? Skip it
        next
      end
      maprdb_record = table.find_by_id(json_record['query'])
      if maprdb_record
        maprdb_record = JSON.parse(maprdb_record.as_json_string)
        # If it exists, increment the count and last seen value along with the object of hosts that have queried
        maprdb_record['count'] += 1
        maprdb_record['last_seen'] = json_record['ts']

        #puts "Updating Hosts"
        maprdb_record['hosts'].push(json_record['id_orig_h']) unless maprdb_record['hosts'].include?(json_record['id_orig_h'])

        table.insert_or_replace(maprdb_record.to_json)
      else
        # If it doesn't exist, create the default record and pass a "new name" alert onto the topic for alerts
        puts "Record doesn't exist, creating it"
        new_record = MapRDB.new_document.set_id(json_record['query']).set("count", 1).set('first_seen', json_record['ts']).set('last_seen', json_record['ts']).set('hosts', [json_record['id_orig_h']]).set('answers', [json_record['answers']])
        table.insert_or_replace(new_record)
      end
    end
  end
end