echo "------------------------------------ Creating Network"
docker network create chain-net

echo "------------------------------------ Launching containers"
docker run -dit --name chain1 --rm --hostname chain1 --network chain-net chain-paxos
docker run -dit --name chain2 --rm --hostname chain2 --network chain-net chain-paxos
docker run -dit --name chain3 --rm --hostname chain3 --network chain-net chain-paxos
docker run -dit --name chain4 --rm --hostname chain4 --network chain-net chain-paxos
docker run -dit --name chain5 --rm --hostname chain5 --network chain-net chain-paxos

echo "------------------------------------ Starting ChainPaxos"
docker exec chain1 java -cp chain.jar:. app.HashMapApp interface=eth0 algorithm=chain_delayed initial_state=ACTIVE initial_membership=chain1,chain2,chain3,chain4,chain5 quorum_size=3 2>&1 | sed "s/^/[chain1] /" &
docker exec chain2 java -cp chain.jar:. app.HashMapApp interface=eth0 algorithm=chain_delayed initial_state=ACTIVE initial_membership=chain1,chain2,chain3,chain4,chain5 quorum_size=3 2>&1 | sed "s/^/[chain2] /" &
docker exec chain3 java -cp chain.jar:. app.HashMapApp interface=eth0 algorithm=chain_delayed initial_state=ACTIVE initial_membership=chain1,chain2,chain3,chain4,chain5 quorum_size=3 2>&1 | sed "s/^/[chain3] /" &
docker exec chain4 java -cp chain.jar:. app.HashMapApp interface=eth0 algorithm=chain_delayed initial_state=ACTIVE initial_membership=chain1,chain2,chain3,chain4,chain5 quorum_size=3 2>&1 | sed "s/^/[chain4] /" &
docker exec chain5 java -cp chain.jar:. app.HashMapApp interface=eth0 algorithm=chain_delayed initial_state=ACTIVE initial_membership=chain1,chain2,chain3,chain4,chain5 quorum_size=3 2>&1 | sed "s/^/[chain5] /" &

sleep 60

echo "------------------------------------ Starting single client"

docker run --name client1 --rm --hostname client1 --network chain-net chain-client -threads 20 -p fieldlength=100 -p hosts=chain1,chain2,chain3,,chain4,chain5 -p readproportion=0.5 -p updateproportion=0.5 -p maxexecutiontime=40

echo "------------------------------------ Stopping containers"
docker exec chain1 killall java
docker exec chain2 killall java
docker exec chain3 killall java
docker exec chain4 killall java
docker exec chain5 killall java

docker kill chain1 chain2 chain3 chain4 chain5
