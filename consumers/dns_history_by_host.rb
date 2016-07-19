# DNS History By Host: Reads Bro DNS events from the MapR Stream and updates aggregate information in MapR-DB
# TODO: Change to MapR-DB Mutations instead of updating the whole document (concurrency issues)
# TODO: Write Stats to a statistics stream
# TODO: general cleanup

include Java
require_relative '../lib/streams_consumer'
require_relative '../lib/maprdb'
require 'json'

consumer = StreamsConsumer.new('dns_by_host', ["/bro-events:dns"])
table = MapR_DB.new("hosts")

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
      maprdb_record = table.find_by_id(json_record['id_orig_h'])
      if maprdb_record
        maprdb_record = JSON.parse(maprdb_record.as_json_string)
#               puts "MapRDB: #{maprdb_record.inspect}"
        if maprdb_record['dns_queries']
          if maprdb_record['dns_queries'][json_record['query']]
            maprdb_record['dns_queries'][json_record['query']]['count'] += 1
            maprdb_record['dns_queries'][json_record['query']]['last_queried'] = json_record['ts'].to_f unless maprdb_record['dns_queries'][json_record['query']]['last_queried'].to_f >= json_record['ts'].to_f
          else
            maprdb_record['dns_queries'][json_record['query']] = { 'count' => 1, 'last_queried' => json_record['ts'].to_f, 'first_queried' => json_record['ts'].to_f}
          end
        else
          maprdb_record['dns_queries'] = {json_record['query'] =>  {'count' => 1, 'last_queried' => json_record['ts'].to_f, 'first_queried' => json_record['ts'].to_f}}
        end
#puts "Inserting: "
#puts maprdb_record.inspect
        table.insert_or_replace(maprdb_record.to_json)
      else
        # If it doesn't exist, create the default record and pass a "new name" alert onto the topic for alerts
        puts "Record doesn't exist, creating it"
#               new_record = { '_id' => json_record['query'], 'count' => 1, 'last_seen' => json_record['ts'], 'hosts' => [json_record['id_orig_h']], 'answers' => [json_record['answers'] => 1]}.to_json
#               puts new_record
        new_record = { '_id' => json_record['id_orig_h'], 'dns_queries' => {json_record['query'] =>  {'count' => 1, 'last_queried' => json_record['ts'].to_f, 'first_queried' => json_record['ts'].to_f}}}.to_json
#puts new_record.inspect
        table.insert_or_replace(new_record)
      end
    end
  end
end
