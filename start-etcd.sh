echo "Starting infra1"
etcd --name infra1 --listen-client-urls http://127.0.0.1:2379 \
    --advertise-client-urls http://127.0.0.1:2379 --listen-peer-urls http://127.0.0.1:12380 --initial-advertise-peer-urls http://127.0.0.1:12380 \
    --initial-cluster-token etcd-cluster-1 --initial-cluster 'infra1=http://127.0.0.1:12380,infra2=http://127.0.0.1:22380,infra3=http://127.0.0.1:32380' \
    --initial-cluster-state new --enable-pprof

echo "Starting infra2"
etcd --name infra2 --listen-client-urls http://127.0.0.1:22379 \
    --advertise-client-urls http://127.0.0.1:22379 --listen-peer-urls http://127.0.0.1:22380 --initial-advertise-peer-urls http://127.0.0.1:22380 \
    --initial-cluster-token etcd-cluster-1 --initial-cluster 'infra1=http://127.0.0.1:12380,infra2=http://127.0.0.1:22380,infra3=http://127.0.0.1:32380' \
    --initial-cluster-state new --enable-pprof

echo "Starting infra3"
etcd --name infra3 --listen-client-urls http://127.0.0.1:32379 \
    --advertise-client-urls http://127.0.0.1:32379 --listen-peer-urls http://127.0.0.1:32380 --initial-advertise-peer-urls http://127.0.0.1:32380 \
    --initial-cluster-token etcd-cluster-1 --initial-cluster 'infra1=http://127.0.0.1:12380,infra2=http://127.0.0.1:22380,infra3=http://127.0.0.1:32380' \
    --initial-cluster-state new --enable-pprof

etcd grpc-proxy start --endpoints=127.0.0.1:2379,127.0.0.1:22379,127.0.0.1:32379 \
    --listen-addr=127.0.0.1:23790 --advertise-client-url=127.0.0.1:23790 \
    --enable-pprof
