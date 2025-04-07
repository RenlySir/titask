CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o tibatch

./task1 -config ctask.toml 2>&1 | tee run.log