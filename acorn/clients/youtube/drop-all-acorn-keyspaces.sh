my_pub_ip=`curl -s http://169.254.169.254/latest/meta-data/public-ipv4`

cqlsh -e "drop keyspace acorn_pr;"       $my_pub_ip
cqlsh -e "drop keyspace acorn_attr_pop;" $my_pub_ip
cqlsh -e "drop keyspace acorn_obj_loc;"  $my_pub_ip
cqlsh -e "drop keyspace acorn_sync;"     $my_pub_ip
cqlsh -e "drop keyspace acorn_regular;"  $my_pub_ip
