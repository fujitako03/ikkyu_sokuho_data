FROM_DATE="2021-04-07"
TO_DATE="2021-04-08"

TEMP_DATE=$FROM_DATE
while [ 1 ] ; do
    # 何かの処理
    echo $TEMP_DATE
    gcloud pubsub topics publish suponavi-scheduler --message '{"start_date": "'$TEMP_DATE'", "end_date": "'$TEMP_DATE'"}'
    
    # ENDDATE分まで処理したら終わり
    if [ $TEMP_DATE = $TO_DATE ] ; then
        break
    fi
    
    # 日付をインクリメント
    TEMP_DATE=`date -d "$TEMP_DATE 1day" "+%Y-%m-%d"`
done