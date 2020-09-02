Export Hadoop metrics to graphite.

# Deploy

## Docker

```
docker build -t hadoop-graphite-reporter . 
docker run -v /tmp/cluster.json:/etc/cluster.json:ro \
  --env GRAPHITE_HOST=localhost \
  --env GRAPHITE_PORT=2003 \
  --env GRAPHITE_PREFIX=hadoop \
  --name hadoop-graphite-reporter hadoop-graphite-reporter:lastest 
```