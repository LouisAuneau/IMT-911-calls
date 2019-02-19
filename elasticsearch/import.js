var elasticsearch = require('elasticsearch');
var csv = require('csv-parser');
var fs = require('fs');

// Elasticsearch client
var esClient = new elasticsearch.Client({
  host: 'localhost:9200',
  log: 'error'
});

createNewIndex();

// Read CSV
let calls = [];
fs.createReadStream('../911.csv')
    .pipe(csv())
    .on('data', data => {
      // Creating meta-data for each document
      calls.push({
        index: {
          _index: "calls",
          _type: "_doc"
        }
      });
      calls.push(createJSONCall(data));
    })
    .on('end', () => {
      // Insert calls in elasticsearch
      esClient.bulk({body: calls}, (err, resp) => {
        if(err) {
          console.error(err)
        } else {
          console.log("Imported "+resp.items.length+" items successfully.");
        }
        esClient.close();
      });
    });


// ----------------------------------------------------------------------------------


/**
 * Create JSON object to be inserted in ES for a given call.
 */
function createJSONCall(data) {
  return {
    location: data.lat+","+data.lng,
    datetime: new Date(data.timeStamp),
    category: data.title.split(":", 2)[0],
    title: data.title.split(":", 2)[1],
    city: data.twp
  };
}

/**
 * Create a new index "calls" with its mapping. Erase it if it was already existing.
 */
function createNewIndex() {
  let exists = esClient.indices.exists({index: "calls"});

  if(exists) {
    esClient.indices.delete({index: 'calls'});
  }

  esClient.indices.create({
    index: 'calls',
    body: {
      settings: {
        index: {
          number_of_replicas: 0
        }
      },
      mappings: {
        _doc: {
          properties: {
            location: {
              type: "geo_point"
            },
            datetime: {
              type: "date"
            },
            category: {
              type: "text"
            },
            title: {
              type: "text"
            },
            city: {
              type: "text"
            }
          }
        }
      }
    }
    });
  }