
const fs = require('fs');
const { Client } = require('pg');
const fetch = require('node-fetch');
const { sleep, batchPromises } = require('./utils');
require('dotenv').config();

/**
 * @typedef {'1hour' | '5min' | '24hour'} Interval
 */

const REQUESTS_PER_SECONDS = 5;

/** @type {Record<Interval, string>} */
const tableByIntervals = {
  '1hour': 'intraday_1hour',
  '5min': 'intraday_5min',
  '24hour': 'intraday_24hour',
};

/**
 * 
 * @typedef {Object} Pagination
 * @prop {number} limit
 * @prop {number} offset
 * @prop {number} count
 * @prop {number} total
 */

/**
 * @typedef {Object} Intraday
 * @prop {string} date
 * @prop {string} symbol
 * @prop {string} exchange
 * @prop {number} open
 * @prop {number} high
 * @prop {number} low
 * @prop {number} close
 * @prop {number} last
 * @prop {number} volume
 *  
 */

/**
 * 
 * @typedef {Object} IResp
 * @prop {Pagination} pagination
 * @prop {Intraday[]} data
 */

/**
 * 
 * @typedef {Object} IErrResp
 * @prop {{ code: string, message: string }} error
 */

/**
 * 
 * @param {Date} date 
 * @param {number} days 
 * @returns {Date}
 */
 function addDays(date, days) {
  const newDate = new Date(date.getTime());
  newDate.setDate(newDate.getDate() + days);
  return newDate;
}

/**
 * @param {Client} client
 * @param {string} tableName
 * @param {string} symbol
 * @param {string} minDate
 * @param {string} maxDate
 * @returns {Promise<void>}
 */
async function deleteExistingDateData(client, tableName, symbol, minDate, maxDate) {
  const query = `
  DELETE FROM ${tableName}
   WHERE symbol = $1
     AND date BETWEEN $2 AND $3`;
  await client.query(query, [symbol, minDate, maxDate]);
}

/**
 * 
 * @param {Client} client 
 * @param {string} tableName 
 * @param {Intraday} record 
 */
async function insertRecord(client, tableName, record) {
  const query = `INSERT INTO ${tableName} (date, symbol, exchange, open, high, low, close, last, volume) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`;
  await client.query(
    query,
    [record.date, record.symbol, record.exchange, record.open, record.high, record.low, record.close, record.last, record.volume]
  );
}

/**
 * 
 * @param {Client} client
 * @param {string} tableName 
 * @param {Intraday[]} records 
 */
async function insertAllRecords(client, tableName, records) {
  await Promise.all(records.map(rec => insertRecord(client, tableName, rec)));
}

/**
 * 
 * @param {Client} client
 * @param {string} symbol
 * @param {Interval} interval 
 * @param {Intraday[]} records 
 */
async function handleData(client, symbol, interval, records) {
  await client.query('BEGIN');
  const tableName = tableByIntervals[interval];
  if (records.length > 0) {
    const minDate = records[0].date;
    const maxDate = records[records.length - 1].date;
    await deleteExistingDateData(client, tableName, symbol, minDate, maxDate);
  }
  await insertAllRecords(client, tableName, records);
  await client.query('COMMIT');
}

/**
 * 
 * @returns {string[]}
 */
async function getSymbols() {
  const data = await fs.promises.readFile('./symbols.txt', 'utf8');
  return data.split('\n');
}

/**
 * @param {string} symbol
 * @param {string} date_from
 * @param {string} date_to
 * @param {number} limit
 * @param {Interval} interval
 * @param {string} sort
 * @returns {Promise<IResp>}
 */
async function fetchIntradayData(
  symbol,
  interval, // = '5min',
  date_from,
  date_to,
  limit = 1000,
  sort = 'ASC',
  offset = 0,
) {
  const urlParams = new URLSearchParams({
    access_key: process.env.MARKETSTACK_ACCESS_KEY,
    symbols: symbol,
    date_from,
    date_to,
    limit,
    sort,
    interval,
    offset,
  });
  const baseUrl = `https://api.marketstack.com/v1/intraday?${urlParams}`;
  const resp = await fetch(baseUrl);
  if (!resp.ok) {
    /** @type {IErrResp} */
    const jsonErrResp = await resp.json();
    console.log('fetchIntradayData err', {
      symbol, date_from, date_to, limit, sort, interval, offset
    }, resp.status, resp.statusText, jsonErrResp, new Date());
    const errorMessage = `fetchIntradayData err ${JSON.stringify({ symbol, date_from, date_to, limit, sort, interval, offset, jsonErrResp })}, ${resp.status}, ${resp.statusText}, @${new Date().toJSON()}`;
    return Promise.reject(Error(errorMessage));
  }
  return resp.json();
}

const MAX_MS_BY_SYMBOL = 1000;
const FETCH_REQUEST_LIMIT = 1000;

/**
 * 
 * @param {Client} client 
 * @param {string} symbol 
 * @param {Interval} interval 
 * @param {string} dateFrom 
 * @param {string} dateTo
 * @param {number=0} offset @default 0
 * @returns {Promise<void>}
 */
async function handleSymbol(client, symbol, interval, dateFrom, dateTo, offset = 0) {
  console.log('handleSymbol', { symbol, interval, dateFrom, dateTo, offset }, new Date().toLocaleString());
  const beganAt = Date.now()
  /** @type {IResp|IErrResp} */
  const resp = await fetchIntradayData(symbol, interval, dateFrom, dateTo, FETCH_REQUEST_LIMIT, 'ASC', offset);
  await handleData(client, symbol, interval, resp.data);
  const diff = Date.now() - beganAt;
  if (diff < MAX_MS_BY_SYMBOL) {
    await sleep(MAX_MS_BY_SYMBOL - diff);
  }
  if (resp.pagination.count < FETCH_REQUEST_LIMIT) {
    return;
  }
  return handleSymbol(client, symbol, interval, dateFrom, dateTo, offset + FETCH_REQUEST_LIMIT);
}

/**
 * 
 * @param {Interval} interval
 * @param {string} dateFrom 
 * @param {string} dateTo 
 */
async function processRange(interval, dateFrom, dateTo) {
  const client = new Client();
  await client.connect();
  const symbols = await getSymbols();
  const reqs = symbols.map(symbol => () => handleSymbol(client, symbol, interval, dateFrom, dateTo));
  await batchPromises(reqs, REQUESTS_PER_SECONDS - 1, 1000)
  await client.end();
}

/**
 * 
 * @param {Date} date 
 * @returns {string}
 */
const formatDateStr = (date) => date.toJSON().slice(0, 19) + '+0000';

function init() {
  console.log('Inicio @', new Date().toJSON())
  const interval = '5min';
  const now = new Date();
  const dateFrom = formatDateStr(addDays(now, -365.25 * 10));
  const dateTo = formatDateStr(now);
  processRange(interval, dateFrom, dateTo)
    .then(console.log, console.error)
    .finally(() => console.log('FIN @', new Date().toJSON()));
}

init();