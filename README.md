# Neo4j batch index stream

A writable stream for batch indexing labeled nodes and relations into Neo4j. For example from a csv file or piped output. Before streaming, it will check if a connection to neo4j can be made, and optionally indexes can be set to speed up the inserting process.


### Usage

Given the following `test.csv` file:

```//// test.csv

Fabien|likes|Nick
Nick|dislikes|Fabien
Jochem|likes|Nick
Lydia|likes|Fabien
... etc
```


One could run `less test.csv | node.js main.js` to pipe it through the following script that uses `neo4j-batch-index-stream`.

```
//// main.js

var through2 = require('through2');
var split2 = require('split2');
var Neo4jStream = require('neo4j-batch-index-stream');

var buildRecords = through2({ objectMode: true }, function(chunk, enc, callback) {
    var line = chunk.toString().trim();

    if (line && line.length) {
        var parts = line.split("|");

        var Left = {
            "label": "Person",
            "name" : parts[0],
        }

        var Right = {
            "label" : "Person",
            "name" : parts[2],
        };

        var relation = {
            "relation" : parts[1],
            "start"    : Left,
            "end"      : Right,
        }

        this.push(Left);
        this.push(Right);
        this.push(relation);
    }

    callback();
});


var config = require("./secret-config.json");
var username = config.username;
var password = config.password; // your own password ;


var stream = new Neo4jStream(username, password, {
    "index_key"     : "name",
    "highWaterMark" : 10000
});

stream.index([
    ["Person", "name"]
]);


process.stdin
.pipe(split2())
.pipe(buildRecords)
.pipe(stream)
.on('error', function(error) {
    console.log(error);
})
.on('finish', function() {
    console.log("DONE");
})
```

![Neo4j database with people and relations](./neo4j_example.png)


### Errors

`Error: Error: connect ECONNREFUSED 127.0.0.1:7474`
> Neo4j is not running or not available.

<hr>

```
Error: {
  "errors" : [ {
    "message" : "Invalid username or password.",
    "code" : "Neo.ClientError.Security.AuthorizationFailed"
  } ]
}
```
> You either have the default neo4j/neo4j password or gave incorrect credentials.
