const express = require('express');
const { startStream } = require('./src');
const app = express();

const port = process.env.PORT || 4123;
const server = app.listen(port, () => console.log(`[start] Listening on ${port}`));

setTimeout(() => {
  startStream();
}, 1000);