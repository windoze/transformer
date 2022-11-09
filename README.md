Test script:

```
curl -v -XPOST -H"content-type:application/json" localhost:8000/process -d'{
    "requests": [
        {
            "pipeline": "t1",
            "data": {
                "fv": 97,
                "os": "Windows",
                "appBundle": "foo.bar.someApp",
                "osVersion": "11.0.1"
            }
        }
    ]
}'
```

