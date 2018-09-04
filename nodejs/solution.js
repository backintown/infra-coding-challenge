const Redis = require('ioredis');
const elasticsearch = require('./clients/elasticsearch');

const { createRecord, setupElasticsearch } = require('./utils');

(async () => {
    await setupElasticsearch();
    await seedRedis(25000);
    await importRecords();
})();

async function importRecords() {
    console.log('importing records from redis -> elasticsearch');

    const redis = new Redis({ host: 'redis' });
    let record;
    let chunk = [];
    let j = 0;
    do {
        record = await redis.rpop('records');
        // record && records.push(record);
        if (record) {
            chunk.push({
                index: {
                    _index: 'records',
                    _type: 'all'
                }
            })
            chunk.push(record);
        }
        j++;
        if (j % 50 === 0) {
            console.log('importing');
            await elasticsearch.bulk({ body: chunk }).then(_ => {
                chunk = [];
            });
        }
    } while (record);

    await redis.disconnect();
}

async function seedRedis(numRecords) {
    const redis = new Redis({ host: 'redis' });

    console.log('flushing redis');
    await redis.flushall();

    for (let i = 0; i < numRecords; i += 1) {

        await redis.rpush('records', JSON.stringify(createRecord()));

        if (i % (numRecords / 10) === 0) {
            console.log(`${i} records seeded in redis`);
        }
    }

    console.log('done seeding redis');
    await redis.disconnect();
}