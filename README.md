Neo4j batch index stream
==========================

A writable stream, using `flushwritable`, to allow batch indexing labeled nodes and their relations into Neo4j. Useful if you have many specific nodes (< 10 milion nodes) that have their own label, attributes, and relationship types. The `Neo4j batch import tool` is very good, but in this use case you have to build many seperate csv files for all node types and relations between them.


### Usage

`npm install neo4j-batch-index-stream`


Given the following `test.csv` file:

```
Fabien|likes|Nick
Nick|dislikes|Fabien
Jochem|likes|Nick
Lydia|likes|Fabien
```


And the following example script, that uses `neo4j-batch-index-stream`. You can then run `less test.csv | node.js main.js` to pipe the contents of test.csv into main.js, which converts the csv into objects that can be batch inserted in neo4j. Before streaming, it will check if a connection to neo4j can be made, and optionally indexes can be set to speed up the inserting process.


```javascript
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

#### Result

![Neo4j database with people and relations](./test/neo4j_example.png)


### Possible errors

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


### Contributing

If you have neo4j running, you can do a simple batch insert with `npm test`.

If you feel something is missing, you can open an issue stating the problem and desired result. If code is unclear give me a @mention. Pull requests are welcome.