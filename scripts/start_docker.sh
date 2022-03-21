cd $HOME

# Try starting docker unless it is successful.
MAX_TRIES=4
t=0
flag=0

pgrep dockerd | sudo xargs kill
pgrep containerd-shim | sudo xargs kill

tar -xvf docker-19.03.12.tgz

# Remove previous versions of docker, containerd, runc
sudo rm /usr/bin/containerd*
sudo rm /usr/bin/docker*
sudo rm /usr/bin/runc*
sudo rm /usr/bin/ctr*

# Copy executables onto a path
sudo cp docker/* /usr/bin/

while [ $t -lt ${MAX_TRIES} ]; do  
  sudo dockerd > /dev/null 2>&1 &
  sleep 2
  sudo docker ps -a
  status=$?
  if [ "$status" == "0" ]; then
    flag=1
    break
  fi
  t=$((t+1))
done

if [ "$flag" == "1" ]; then
  echo "Docker start successful"
else
  echo "Docker start unsuccessful"
fi