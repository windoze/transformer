Build the program
-----------------

```
./gradlew shadowJar
```

Run the program
---------------

```
java -jar build/libs/app.jar -p test/test.conf -l test/lookup-source.json
```

Test script:
------------

```shell
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

```shell
curl -v -XPOST -H"content-type:application/json" localhost:8000/process -d'{
    "requests": [
        {
            "pipeline": "t1",
            "validate": true,
            "data": {
                "fv": 15,
                "os": "Windows",
                "appBundle": "foo.bar.someApp",
                "osVersion": "11.0.1"
            }   
        },
        {
            "pipeline": "t3",
            "validate": true,
            "data": {
                "ip": ["1.1.1.1", "24.48.0.1", "8.8.8.8"]
            }   
        }
    ]
}'

```