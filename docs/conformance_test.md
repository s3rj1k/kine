# Setting Up K3s with Kine and Running Conformance Tests

## Building and Starting Kine (Console #1)

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

## Starting K3s Server (Console #2)

[K3s Server CLI](https://docs.k3s.io/cli/server)
[K3s Releases](https://github.com/k3s-io/k3s/releases)
[Running K3s in Docker](https://docs.k3s.io/advanced#running-k3s-in-docker)

### Create K3s config

```bash
mkdir -p /etc/rancher/k3s && cat << EOF > /etc/rancher/k3s/config.yaml
kube-apiserver-arg:
  - "feature-gates=WatchList=true"
write-kubeconfig-mode: "0644"
datastore-endpoint: "http://127.0.0.1:2379"
EOF
```

### Run K3s server

```bash
k3s server --debug
```

## Running Conformance Tests (Console #3)

### Install [Hydrophone](https://github.com/kubernetes-sigs/hydrophone)

```bash
go install sigs.k8s.io/hydrophone@latest
```

### Get `kubeconfig`

```bash
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
```

### Run full conformance tests

```bash
~/go/bin/hydrophone --conformance
```

### Run conformance test with focus

```bash
~/go/bin/hydrophone --focus sig-api-machinery
```
