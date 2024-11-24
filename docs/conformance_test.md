# Setting Up K3s with Kine and Running Conformance Tests

## Building and Starting Kine (Console #1)

### Option 1: Using SQLite

```bash
make build && \
    rm -rf db && \
    ./bin/kine \
        --endpoint="sqlite://db?cache=shared&mode=rwc" \
        --watch-progress-notify-interval=5s \
        --listen-address=0.0.0.0:2379 \
        --skip-verify=true \
        --debug
```

### Option 2: Using Local File System

```bash
make build && \
    rm -rf db && \
    ./bin/kine \
        --endpoint="localfs://$(pwd)/db" \
        --watch-progress-notify-interval=5s \
        --listen-address=0.0.0.0:2379 \
        --skip-verify=true \
        --debug
```

## Starting K3s Server (Console #2)

[K3s Server CLI](https://docs.k3s.io/cli/server)
[Running K3s in Docker](https://docs.k3s.io/advanced#running-k3s-in-docker)

```bash
docker run \
    --interactive \
    --tty \
    --rm \
    --name k3s-server \
    --network host \
    --privileged \
    --env K3S_DEBUG=true \
    --env K3S_DATASTORE_ENDPOINT='http://localhost:2379' \
    docker.io/rancher/k3s:v1.31.7-k3s1 server \
        --kube-apiserver-arg=feature-gates=WatchList=true \
        --disable=coredns,servicelb,traefik,local-storage,metrics-server \
        --disable-network-policy \
        --write-kubeconfig-mode "0644"
```

## Running Conformance Tests (Console #3)

### Install [Hydrophone](https://github.com/kubernetes-sigs/hydrophone)

```bash
go install sigs.k8s.io/hydrophone@latest
```

### Get `kubeconfig`

```bash
export K3S_VERSION=$(docker exec k3s-server /bin/sh -c "/bin/k3s --version | grep -o 'v[0-9]\+\.[0-9]\+\.[0-9]\+'")
docker cp k3s-server:/etc/rancher/k3s/k3s.yaml /root/.kube/config
```

### Run full conformance tests

```bash
~/go/bin/hydrophone --conformance
```

### Run conformance test with focus

```bash
~/go/bin/hydrophone --focus sig-api-machinery
```
