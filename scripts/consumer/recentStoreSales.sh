start=`date -jf "%Y-%m-%d %H:%M:%S" "2023-01-04 14:45:00" +%s`
end=`date -jf "%Y-%m-%d %H:%M:%S" "2023-09-08 14:45:00" +%s`
echo ${start}
startTime=$((start*1000))
echo ${startTime}
endTime=$((end*1000))
echo ${endTime}
curl -X GET -H "Content-Type: application/json" -H "Accept: application/json" "http://localhost:8082/recentStoreSales?store=33333&queue=QV1&startTime=${startTime}&endTime=${endTime}"
