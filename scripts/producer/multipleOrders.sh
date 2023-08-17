for f in `ls multipleOrders/*.json`; do
   curl -X POST -H "Content-Type: application/json" http://localhost:8080/produce --data @${f}
done
