const Promise = require('bluebird');
const db = require('../helpers/db');
const log = console.log;

/** Work to do before streaming the stable chain */
const init = () => new Promise((resolve, reject) => {
    Promise.delay(100).then(() => {
        log('Stable init');
        resolve();
    });
});

/** Work to do for each stable block */
const handleBlock = (block, blockNum) => new Promise((resolve, reject) => {
    //can do something before new block transactions are processed
    resolve();
});

/** Work to do for each stable transaction */
const handleTransaction = (op, i, tx, block, blockNum) => new Promise((resolve, reject) => {
    const [type, payload] = op;
    log('Transaction ', type, ' with tx id: ', tx.transaction_id, ' ts: ', Date.parse(block.timestamp) / 1000, ' on block ', blockNum, ' is being processed');
    switch (type) {
        case "custom_json": {
            const query = `INSERT INTO custom_json (payload) VALUES (?);`;
            db.queryAsync(query, [payload]).then(result => {
                resolve(result);
            }).catch((err) => {
                console.error('Failed to add custom_json', err);
                reject(err);
            });
            break;
        }
        case 'comment': {
            resolve();
            break;
        }
        default: {
            resolve();
            break;
        }
    }
});

module.exports = {
    init,
    handleBlock,
    handleTransaction,
};