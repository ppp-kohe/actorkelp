## JSON API

* default port: `48888 ` 
* configure: `ConfigDeployment.{httpHost, httpPort}`
* The server is provided as `ClusterHttp` taking `ClusterDeployment` (actually the sub-class `ClusterKeyAggregation`)
    * the server lists up API methods in the `ClusterDeployment` instance whose name has the annotation `@PropertyInterface(...)` 
    * The parameter `value` of `@PropertyInterface` becomes API path. e.g. `@PropertyInterface("cluster")`  becomes `/cluster`
    * It can duplicated values as overloading methods with different parameters
    * The parameters and the returned value of API methods are converted and transferred as JSON objects
* API URL becomes like ` http://host:48888/<method-name>?a0=<arg0>&a1=<arg1>&... `
    * Non-JSON objects with implementing the `ToJson` interface can be converted to a JSON object by it's `toJson` method.
    * `Class` becomes a name string
    * `Instant` and `Duration` are converted to string: `2020-03-13T23:54:51.461644Z`, or `PT1H12.345S`
    * `ActorRef`, `ActorAddress`: `host:port/name`. Note: name of a leaf in the actor tree becomes like `name#0101`. for URL query parameters,`#` must be `%23`, like  `name%230101`.
    * Unknown object is converted to a string by `Objects.toString`

### (template)

* input: `()`
* output: `T`

```javascript
//output example
{
  
}
```

### cluster

* input: `()`
* output: `ClusterStats`

```javascript
{
  "appName": String,
  "defaultConfType": String //Class,
  "nodeMainType": String //Class,
  "placeType": String //Class,
  "primary": { //ClusterUnit
    "name": String,
    "deploymentConfig": {
        "ssh": "ssh %s",
        "java": "java %s %s %s",
        "host": "localhost",
        "port": 38888,
        "configType": "csl.actor.keyaggregate.Config",
        "baseDir": "target/debug",
        "primary" : true,
        "sharedDeploy": true,
        "joinTimeoutMs": 10000,
        "pathSeparator": ":", //File.pathSeparator
        "appNameHeader": "app",
        "logFile": true,
        "logFilePath"  : "%a/stderr-%h.txt",
        "logFilePreserveColor": true,
        "httpPort": 48888
    },
    "appConfig": { //Config
      "mailboxThreshold": 1000,
      "mailboxTreeSize": 32,
      "lowerBoundThresholdFactor": 0,001;
      "minSizeOfEachMailboxSplit": 10,
      "maxParallelRoutingThresholdFactor": 5.0;
      "historyEntrySize": 10,
      "historyEntryLimitThresholdFactor": 0.1;
      "historyExceededLimitThresholdFactor": 0,3;
      "mergeRatioThreshold": 0.2;
      "pruneGreaterThanLeafThresholdFactor": 2.0,
      "pruneLessThanNonZeroLeafRate": 0.2;
      "toLocalWaitMs": 20000,
      "logSplit": true,
      "logColor": 17,
      "logColorPhase": 27,
      "traverseDelayTimeMs": 300,
      "persist": false,
      "persistMailboxPath": "%a-persist-%h",
      "persistRuntimeCondition": true,
      "persistMailboxSizeLimit": Integer,MAX_VALUE / 512;
      "persistMailboxOnMemorySize": 100_000L,
      "reduceRuntimeCheckingThreshold": 100_000,
      "reduceRuntimeRemainingBytesToSizeRatio": 0.003,
      "histogramPersistHistoryEntrySize": 10,
      "histogramPersistHistoryEntryLimit": 100,
      "histogramPersistSizeLimit": 100000,
      "histogramPersistOnMemorySize": 100,
      "histogramPersistSizeRatioThreshold": 0.00001,
      "histogramPersistRandomSeed": 0,
      "fileMapperSplitByCount": false,
      "fileMapperSplitLength": 100000,
      "fileMapperSplitCount": 10,
      "logHeader": "",
    }
    "classPathList": [
      String
    ],
    "block": String //CommandBlock
  },
   "nodes": [
     { //ClusterUnit
       ...//same as "primary"
     }
   ]
}
```



### placement-move

* input: `(ActorRef actor, ActorAddress host)`
    * `host:port/name`
    * `host:port`
* output: `ActorRef`

```javascript
"host:port/name"
```



### actor-load-and-send

* input: `(ActorRef actor, String path)`
    * `host:port/name`
    * `filePath`
* output: `()`

```javascript
null
```



### placement-shutdown

* input: `(ActorAddress host)`
    * `host:port`
* output: `()`

```javascript
null
```



### cluster-shutdown-all

* input: `()`
* output: `()`

```javascript
null
```

### placement

* input: `()`
* output: `ActorRef`

```javascript
"host:port/name"
```

### placement-config-primary

* input: `()`
* output: `ConfigBase`

```javascript
{ //Config
  ... //same as cluster().primary.appConfig
}
```

### system

* input: `()`, `(ActorAddressRemote host)`
    * `host:port`
* output: `SystemStats`

```javascript
{ //SystemStats
  "throughput": 15,
  "threads": long,  
  "systemToString": String,
  "placementStreategy": String
}
```

### system-actors

* input: `()`, `(ActorAddressRemote host)`
    * `host:port`
* output: `Map<String,ActorRef>`

```javascript
{ 
  "name": "host:port/name"
}
```

### system-process-count

* input: `()`, `(ActorAddressRemote host)`
    * `host:port`
* output: `int`

```javascript
int
```

### system-connections

* input: `()`, `(ActorAddressRemote host)`
    * `host:port`
* output: `Map<ActorAddress,NetworkStats>`

```javascript
{
  "host:port/name": { //NetworkStats
    "address": "host:port/name",
    "count": long,
    "messages": long,
    "time": String, //Duration
    "bytes": long,
    "errors": long
  }
}
```

### system-server-receive

* input: `()`, `(ActorAddressRemote host)`
    * `host:port`
* output: `Map<ActorAddress,NetworkStats>`

```javascript
{ //NetworkStats
  ...//same sa system-connections().value
}
```

### placement-total-threads

* input: `()`, `(ActorAddressRemote host)`
    * `host:port`
* output: `int`

```javascript
int
```

### placement-clusters

* input: `()`, `(ActorAddressRemote host)`
    * `host:port`
* output: `List<AddressListEntry>`

```javascript
[ 
  {//AddressListEntry
		"placementActor": "host:port/name",
    "threads": int
	}
]
```

### placement-clusters-with-self

* input: `()`, `(ActorAddressRemote host)`
    * `host:port`
* output: `List<AddressListEntry>`

```javascript
[ 
  {//AddressListEntry
		...//same as placement-cluster()[0]
	}
]
```

### placement-created-actors

* input: `()`, `(ActorAddressRemote host)`
    * `host:port`
* output: `long`

```javascript
long
```

### placement-remote-config

* input: `()`, `(ActorAddressRemote host)`
    * `host:port`
* output: `Map<ActorAddress,ConfigBase>`

```javascript
{
  "host:port/name":{ //Config
    ...//same as  cluster().primary.appConfig
  }
}
```

### actor-stats

* input:  `(ActorRef actor)`
    * `host:port/name`
* output: `ActorStat`

```javascript
{ //ActorStat
  "ref": "host:port/name",
  "name": "name",
  "className": String, //Class
  "stateType": "router", //or "unit", "canceled", "phaseTerminal"
  "processCount": long,
  "outputFileHeader": String,
  "mailboxSize": long,
  "mailboxThreshold": long,
  "mailboxPersistable": boolean,
  "histogram": [
    { //HistogramStat
      "entryId": int,
      "nextSchedule": String, //Instant
      "lastTraversal": String, //Instant
      "persistable": boolean,
      "valueSize": long,
      "leafSize": long,
      "leafSizeNonZero": long,
      "perssitedSize", long,
      "persistHistoryTotalMean": [
        double
      ]
    }
  ],
  "maxHeight": int, //only if "stateType"=="router"
  "height": int, //only if "stateType"=="router"
  "perallelRouting": boolean, //only if "stateType"=="router"
  "canceled": ["host:/port:name"], //only if "stateType"=="router"
  "phase": [ //only if "stateType"=="router" or "stateType"=="phaseTerminal"
    { //PhaseStat
      "key": any, 
      "startTime": String, //Instant
      "endTime": String or null, //Instant 
      "target": "host:port/name", 
      "finished": {
        "host:port/name": Boolean 
      }
    }
   ],
  "nextStage": "host:port/name"
}
```

### actor-split

* input:` (ActorRef actor, String path)`
    * `host:port/name`
    * `010101` //SplitPath, 0:right, 1:left
* output: `RoutingSplitStat`

```javascript
{ //RoutingSplitStat
  "type": "node", //or "leaf"
  "path": "010101", //SplitPath
  "depth": int,
  "history" : [
    [long,long] //long[] {left, right}
  ],
  "processCount": long,
  "processPoints": [String]
  "actor": "host:port/name"
}
```

### actor-split-tree

* input:` (ActorRef actor)`
    * `host:port/name`
* output: `RoutingSplitStat`

```javascript
{
  "": //key is a path: SplitPath
  { //RoutingSplitStat
    ...//same as actor-split(actor,key)
  },
  "0101":{ //RoutingSplitStat
    ...
  }
}
```



### router-split

* input:` (ActorRef actor, int height)`
    * `host:port/name`
    * `int`
* output: `()`

```javascript
null
```

### router-merge-inactive

* input:` (ActorRef actor)`
    * `host:port/name`
* output: `()`

```javascript
null
```

### router-split-or-merge

* input:` (ActorRef actor, int height)`
    * `host:port/name`
    * `int`
* output: `()`

```javascript
null
```

