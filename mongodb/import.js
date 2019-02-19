var mongodb = require('mongodb');
var csv = require('csv-parser');
var fs = require('fs');

var MongoClient = mongodb.MongoClient;
var mongoUrl = 'mongodb://localhost:27017/911-calls';

var insertCalls = function(db, callback) {
    var collection = db.collection('calls');
    if(collection.findOne()!="null")
        collection.drop();
    var calls = [];
    fs.createReadStream('../911.csv')
        .pipe(csv())
        .on('data', data => {
            var call = createJSONCall(data);
            calls.push(call);
        })
        .on('end', () => {
          collection.insertMany(calls, (err, result) => {
            callback(result)
          });
        });
}

MongoClient.connect(mongoUrl, (err, db) => {
    insertCalls(db, result => {
        console.log(`${result.insertedCount} calls inserted`);
        addIndexes(db);
        db.close();
    });
});

/**
 * Create JSON object to be inserted in Mongo for a given call.
 */
function createJSONCall(data) {
    return {
      location: {
        type: "Point",
        coordinates: [parseFloat(data.lng), parseFloat(data.lat)]
      },
      datetime: new Date(data.timeStamp),
      category: data.title.split(":", 2)[0],
      title: data.title.split(":", 2)[1],
      city: data.twp
    };
}

function addIndexes(db){
    db.collection('calls').createIndex( 
        { location : "2dsphere" }
    );
    db.collection('calls').createIndex( 
        { title : "text" }
    );
}
