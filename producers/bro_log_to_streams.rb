# Bro Log to Streams: Takes a Bro Log as STDIN and writes it to a Streams HTTP REST Server (/servers/streams_rest_server.rb) as a JSON message
# TODO: Need to switch to Ruby hashes and then 'to_json' instead of trying to write a JSON string, this is messy
# TODO: write a version of this that works with the MapR-Streams client library instead of the REST server

require 'net/http'
fields = []
path = ''
#fields ts      uid     id.orig_h       id.orig_p       id.resp_h       id.resp_p       proto   trans_id        query   qclass  qclass_name       qtype   qtype_name      rcode   rcode_name      AA      TC      RD      RA      Z       answers TTLs    rejected

ARGF.each_line do |line|
  if line =~ /^#fields/
    fields = line.chomp.split(/\s+/).map {|x| x.gsub(".", "_")}
    fields.shift
  elsif line =~ /^#path/
    path = line.chomp.split(/\s+/).last
  elsif line[0] == '#'
    next
  else
    next if fields.empty?
    parts = line.chomp.split("\t")
    json_parts = []
    fields.zip(parts).each do |part|
      json_parts.push('"' + part[0] + '"' + ' : "' + part[1] + '"')
    end
    uri = URI('http://192.168.3.101:4567')
    json = "{ #{json_parts.join(", ")} }"
    puts json
    http = Net::HTTP.new('192.168.3.101', 4567)
    res = http.post('/send/bro-events:' + path, json)
  end
end