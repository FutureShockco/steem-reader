const Promise = require('bluebird');
const stable = require('./stream');
const client = require('../helpers/client');
const fs = Promise.promisifyAll(require('fs'));
const readMode = process.env.READ_MODE || 'unstable'
let lastBlockNum = 0;


const stream = setInterval(() => {
  client.database.getDynamicGlobalProperties().then(props => {
    if (readMode === 'stable')
      lastBlockNum = parseInt(props.last_irreversible_block_num);
    else
      lastBlockNum = parseInt(props.head_block_number);
  });
}, 3000);

const startStream = () => {
  stable.init().then(() => {
    fs.readFileAsync('lastblock', { encoding: 'utf8', flag: 'r' }).then(blockHeight => {
      console.log(`Last loaded block was ${blockHeight}`);
      const nextBlockNum = blockHeight ? parseInt(blockHeight) + 1 : 1;
      handleBlock(nextBlockNum);
    }).catch((err) => {
      console.error("Failed to get 'noblock_height' on Redis", err);
    })
  });
};

const handleBlock = (blockNum) => {
  if (lastBlockNum >= blockNum) {
    client.database.getBlock(blockNum).then(block => {
      work(block, blockNum, stable.handleBlock, stable.handleTransaction).then(() => {
        fs.writeFileAsync('lastblock', parseInt(blockNum).toString()).then(blockHeight => {
          console.log(`New block height is ${blockNum} ${block.timestamp}`);
          handleBlock(blockNum + 1);
        }).catch((err) => {
          console.error("Failed to set 'noblock_height' on Redis", err);
          handleBlock(blockNum);
        });
      });
    }).catch(err => {
      console.error(`Request 'getBlock' failed at block num: ${blockNum}, retry`, err);
      handleBlock(blockNum);
    });
  } else {
    Promise.delay(10).then(() => {
      handleBlock(blockNum);
    });
  }
};



/** Work to do at each stable block */
const work = (block, blockNum, handleBlockFn, handleTransactionFn) => new Promise((resolve, reject) => {
  const events = [];
  if (block.transactions && block.transactions.length > 0) {
    block.transactions.forEach(tx => {
      tx.operations.forEach((op, i) => {
        events.push([op, i, tx, block, blockNum]);
      });
    });
  }
  handleBlockFn(block, blockNum).then((result) => {
    if (events.length > 0) {
      Promise.each(events, (event) => {
        return handleTransactionFn(event[0], event[1], event[2], event[3], event[4]);
      }).then((result) => {
        resolve(result);
      }).catch((e) => {
        console.error(`Failed to handle operation on block ${blockNum}`, e);
        reject(e);
      })
    } else {
      console.log(`No work to do on block ${blockNum}`);
      resolve();
    }
  }).catch((e) => {
    console.error(`Failed to handle block ${blockNum}`, e);
    reject(e);
  });
});

module.exports = {
  startStream,
};
