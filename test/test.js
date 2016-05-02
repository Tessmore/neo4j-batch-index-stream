#!/usr/bin/env node

var through2 = require('through2');
var split2 = require('split2');
var Neo4jStream = require('../index.js');

function stamp() {
    return new Date().toISOString()
      .replace(/T/, ' ')
      .replace(/\..+/, '')
}

var buildRecords = through2({ objectMode: true }, function(chunk, enc, callback) {
    var line = chunk.toString().trim();

    // C3887459|A24324894|SCUI|PAR|C1549040|A24326044|SCUI|inverse_isa|R154240736|613001000124120|SNOMEDCT_US|SNOMEDCT_US|0|N|N|8192|

    if (line && line.length) {
        var parts = line.split("|");

        var Right = {
            "label" : "Concept",
            "cui" : parts[0],
        };

        var Left = {
            "label": "Concept",
            "cui" : parts[4],
        }

        var relation = {
            "relation" : parts[7],
            "start"    : Left,
            "end"      : Right,
        }

        this.push(Left);
        this.push(Right);
        this.push(relation);
    }

    callback();
});


var username = "neo4j";
var password = "test123";

var stream = new Neo4jStream(username, password, {
    "index_key"     : "cui",
    "highWaterMark" : 10000
});

stream.index([
    ["Concept", "cui"]
]);

console.log("START", stamp());

process.stdin
.pipe(split2())
.pipe(buildRecords)
.pipe(stream)
.on('error', function(error) {
    console.log(error);
})
.on('finish', function() {
    console.log("DONE", stamp());
})
