# this doesn't work yet
start=`date -jf "%Y-%m-%d %H:%M:%S" "2023-01-04 14:45:00" +%s`
end=`date -jf "%Y-%m-%d %H:%M:%S" "2023-09-08 14:45:00" +%s`
echo ${start}
startTime=$((start*1000))
echo ${startTime}
endTime=$((end*1000))
echo ${endTime}
stores=`./allStoreList.sh`
echo ${stores}
# for store in $stores
for store in "${stores[@]}"
do
    echo ${store}
#     curl -X GET -H "Content-Type: application/json" -H "Accept: application/json" "http://localhost:8082/recentStoreQueueSales?storeQueue=$store}&startTime=${startTime}&endTime=${endTime}"
done
