#!/usr/bin/env node

var through2 = require('through2');
var split2 = require('split2');
var Neo4jStream = require('../index.js');

var nodeID = 1;

var buildRecords = through2({ objectMode: true }, function(chunk, enc, callback) {
    var line = chunk.toString().trim();

    if (line && line.length) {
        var parts = line.split("|");

        var person = {
            "label" : "Person",
            "name"  : parts[0],
            "_id"   : nodeID++,
        };

        var obj = {
            "label": "Object",
            "name" : parts[2],
            "_id"  : nodeID++,
        }

        var relation = {
            "relation" : parts[1],
            "start"    : person._id,
            "end"      : obj._id,
        }

        this.push(person);
        this.push(obj);
        this.push(relation);
    }

    callback();
});


var username = "neo4j";
var password = "test123";

var stream = new Neo4jStream(username, password, {
    highWaterMark: 256
});


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
