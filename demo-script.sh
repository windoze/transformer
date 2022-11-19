#################
# NYC Taxi Demo
#################

# export URL=http://localhost:8000/process
export URL=https://feathr-transformer.azurewebsites.net/process
# export URL=https://piper.azurewebsites.net/process

# nyc_taxi_demo_1_lookup_online_store
curl -s -H"content-type:application/json" $URL -d'{
    "requests": [
        {
            "pipeline": "nyc_taxi_demo_1_lookup_online_store",
            "data": {
                "pu_loc_id": 41,
                "do_loc_id": 57,
                "pu_time": "2020/4/1 0:41",
                "do_time": "2020/4/1 0:56",
                "trip_distance": 6.79,
                "fare_amount": 21.0
            }   
        }
    ]
}' | jq --sort-keys

# nyc_taxi_demo_2_lookup_address
curl -s -H"content-type:application/json" $URL -d'{
    "requests": [
        {
            "pipeline": "nyc_taxi_demo_2_lookup_address",
            "data": {
                "pu_loc_id": 41,
                "do_loc_id": 57,
                "pu_time": "2020/4/1 0:41",
                "do_time": "2020/4/1 0:56",
                "trip_distance": 6.79,
                "fare_amount": 21.0
            }   
        }
    ]
}' | jq --sort-keys

# nyc_taxi_demo_3_local_compute
curl -s -H"content-type:application/json" $URL -d'{
    "requests": [
        {
            "pipeline": "nyc_taxi_demo_3_local_compute",
            "data": {
                "pu_loc_id": 41,
                "do_loc_id": 57,
                "pu_time": "2020/4/1 0:41",
                "do_time": "2020/4/1 0:56",
                "trip_distance": 6.79,
                "fare_amount": 21.0
            }   
        }
    ]
}' | jq --sort-keys

# nyc_taxi_demo
curl -s -H"content-type:application/json" $URL -d'{
    "requests": [
        {
            "pipeline": "nyc_taxi_demo",
            "data": {
                "pu_loc_id": 41,
                "do_loc_id": 57,
                "pu_time": "2020/4/1 0:41",
                "do_time": "2020/4/1 0:56",
                "trip_distance": 6.79,
                "fare_amount": 21.0
            }   
        }
    ]
}' | jq --sort-keys


#################
# InMobi Demo
#################

# inmobi_demo_1_bucket
curl -s -XPOST -H"content-type:application/json" $URL -d'{
    "requests": [
        {
            "pipeline": "inmobi_demo_1_bucket",
            "data": {
                "fv": 15
            }   
        }
    ]
}' | jq --sort-keys

# inmobi_demo_2_concat
curl -s -XPOST -H"content-type:application/json" $URL -d'{
    "requests": [
        {
            "pipeline": "inmobi_demo_2_concat",
            "data": {
                "os": "Windows",
                "appBundle": "foo.bar.someApp"
            }   
        }
    ]
}' | jq --sort-keys


# inmobi_demo_3_split
curl -s -XPOST -H"content-type:application/json" $URL -d'{
    "requests": [
        {
            "pipeline": "inmobi_demo_3_split",
            "data": {
                "osVersion": "11.0.1"
            }   
        }
    ]
}' | jq --sort-keys

# inmobi_demo
curl -s -XPOST -H"content-type:application/json" $URL -d'{
    "requests": [
        {
            "pipeline": "inmobi_demo",
            "data": {
                "fv": 15,
                "os": "Windows",
                "appBundle": "foo.bar.someApp",
                "osVersion": "11.0.1"
            }   
        }
    ]
}' | jq --sort-keys

#################
# GeoIp Demo
#################

curl -s -XPOST -H"content-type:application/json" $URL -d'{
    "requests": [
        {
            "pipeline": "geoip_demo",
            "data": {
                "ip": "8.8.8.8"
            }   
        }
    ]
}' | jq --sort-keys
