import sys,json
from pprint import pprint
from collections import defaultdict 
#with open('test.json') as data_file:
#  data = json.load(data_file)
# raw json string is piped in from standard input 
rawdata = sys.stdin.read()
data = json.loads(rawdata)  

#pprint(data["_meta"]["hostvars"]) 
li = data["_meta"]["hostvars"] 

port = 9160
port1 = 2181
port2 = 2191
str0 = []

#bootstrapDataCenter = 'us-east-1'

#for node in li:
#	if  (data["_meta"]["hostvars"][node]["ec2_tag_is_bootstrap_node"] == "yes" and data["_meta"]["hostvars"][node]["ec2_tag_cluster"] == sys.argv[1]) : 
#	  	bootstrapDataCenter = data["_meta"]["hostvars"][node]["ec2_region"]
#	  	break

for node in li:
	nodeln = int(data["_meta"]["hostvars"][node]["ec2_tag_ring_position"])
	replicaof = int(data["_meta"]["hostvars"][node]["ec2_tag_replica_of"])
	cluster = data["_meta"]["hostvars"][node]["ec2_tag_cluster"]
	str2 = []
	for innernode in li:
		zkmyid = str(int(data["_meta"]["hostvars"][innernode]["ec2_tag_ring_position"]) + 1)
		if (replicaof == int(data["_meta"]["hostvars"][innernode]["ec2_tag_replica_of"]) and data["_meta"]["hostvars"][innernode]["ec2_tag_cluster"] == cluster) : 
			str2.append('server.' + zkmyid + '=zk' + data["_meta"]["hostvars"][innernode]["ec2_tag_ring_position"] + '.' + cluster + '.squigglee' + ':2182:2183:participant;' + 'zk' + data["_meta"]["hostvars"][innernode]["ec2_tag_ring_position"] + '.' + cluster + '.squigglee' + ':' + `port1`)
	file = open("zoo.cfg.dynamic" + str(nodeln),"w+")
	file.write('\n'.join(str2))
	file.close()

for node in li:
	nodeln = int(data["_meta"]["hostvars"][node]["ec2_tag_ring_position"])
	replicaof = int(data["_meta"]["hostvars"][node]["ec2_tag_replica_of"])
	cluster = data["_meta"]["hostvars"][node]["ec2_tag_cluster"]
	str2 = []
	for innernode in li:
		zkmyid = str(int(data["_meta"]["hostvars"][innernode]["ec2_tag_ring_position"]) + 1)
		
		if (data["_meta"]["hostvars"][innernode]["ec2_tag_is_seed"] == "yes" and data["_meta"]["hostvars"][innernode]["ec2_tag_cluster"] == cluster) : 
			str2.append('server.' + zkmyid + '=zk' + data["_meta"]["hostvars"][innernode]["ec2_tag_ring_position"] + '.' + cluster + '.squigglee' + ':2192:2193:participant;' + 'zk' + data["_meta"]["hostvars"][innernode]["ec2_tag_ring_position"] + '.' + cluster + '.squigglee' + ':' + `port2`)

		if (data["_meta"]["hostvars"][innernode]["ec2_tag_is_seed"] == "no" and data["_meta"]["hostvars"][innernode]["ec2_tag_cluster"] == cluster) : 
			str2.append('server.' + zkmyid + '=zk' + data["_meta"]["hostvars"][innernode]["ec2_tag_ring_position"] + '.' + cluster + '.squigglee' + ':2192:2193:observer;' + 'zk' + data["_meta"]["hostvars"][innernode]["ec2_tag_ring_position"] + '.' + cluster + '.squigglee' + ':' + `port2`)	
	
	file = open("zoo.cfg.dynamic.overlay" + str(nodeln),"w+")
	file.write('\n'.join(str2))
	file.close()


