docker run \
        -v $HOME/datahouse/storm/logs:/logs -v $HOME/datahouse/storm/data:/data \
        -d --restart always --name some-nimbus --link some-zookeeper:zookeeper storm storm nimbus 



