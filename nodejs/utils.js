const records = require('../records.json');
const elasticsearch = require('./clients/elasticsearch');

function generateRandomNum(min, max) {
    return Math.floor(Math.random() * (max - min + 1) + min);
}

function createRecord() {
    return records[generateRandomNum(0, records.length - 1)];
}

async function setupElasticsearch() {
    const indexExists = await elasticsearch.indices.exists({ index: 'records' });

    if (indexExists) {
        console.log('deleting records index');
        await elasticsearch.indices.delete({ index: 'records' });
    }

    console.log('creating records index');
    await elasticsearch.indices.create({
        index: 'records',
        waitForActiveShards: '1',
        body: {
            settings: { number_of_shards: 3, number_of_replicas: 0, refresh_interval: -1 },
            mappings: { "all": {} }
        }
    });
}

module.exports = {
    createRecord,
    setupElasticsearch
}