#!/bin/bash
source ~/.bashrc
# time=`date +%Y-%m-%d %H:%M:%S`
day=`date -d "45 min ago" +%Y%m%d`
hour=`date -d "45 min ago" +%H`
minute=`date -d "45 min ago" +%M`
day1=`date -d "30 min ago" +%Y%m%d`
hour1=`date -d "30 min ago" +%H`
minute1=`date -d "30 min ago" +%M`
pre_day=`date -d "15 min" +%Y-%m-%d`
pre_hour=`date -d "15 min" +%H`
pre_minute=`date -d "15 min" +%M`
second=00
time_slide=15
new_minute=`expr $minute / $time_slide \* $time_slide`
new_minute1=`expr $minute1 / $time_slide \* $time_slide`
new_pre_minute=`expr $pre_minute / $time_slide \* $time_slide`
new_minute_=`echo ${new_minute}|awk '{printf("%02d\n",$0)}'`
new_minute1_=`echo ${new_minute1}|awk '{printf("%02d\n",$0)}'`
new_pre_minute_=`echo ${new_pre_minute}|awk '{printf("%02d\n",$0)}'`

dt=$day""$hour""$new_minute_""$second
dt1=$day1""$hour1""$new_minute1_""$second
pre_time=$pre_day" "$pre_hour":"$new_pre_minute_":"$second

echo $dt
echo $dt1
echo $pre_time

sql="
set hive.cli.print.header = true;
select city_id, start_biz_id, car_type, order_num, user_num, picked_num, cancel_num, nodriver_nearby_3km, dt from sy_dw_f.f_agt_thermodynamic_order_new 
where dt = '$dt' or dt = '$dt1'
limit 10000000;
"
hive -e "$sql" > /opt/work/ligk/heatmap/data/time_slide_car.csv
sleep 1s
python /opt/work/ligk/heatmap/car_type/update_num.py /opt/work/ligk/heatmap/data/time_slide_car.csv
sleep 5s
python /opt/work/ligk/heatmap/car_type/car_heatmap.py "$pre_time"
