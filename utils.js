
/**
 * 
 * @param {number} ms
 * @param {any=} val
 * @returns {Promise<any>}
 */
const sleep = (ms, val=undefined) => new Promise(res => setTimeout(res, ms, val));

/**
 * @template T
 * @param {(() => Promise<T>)[]} promises 
 * @param {number} q 
 * @param {number} ms 
 * @param {T[]} defaultValues @default []
 * @returns {Promise<T[]>}
 */
async function batchPromises(promises, q, ms, defaultValues = []) {
  const beganAt = Date.now();
  const sliced = promises.slice(0, q);
  const values = await Promise.allSettled(sliced.map(fn => fn()));
  const newValues = defaultValues.concat(values);
  // console.log('batchPromises newValues', values);
  if (sliced.length < q) {
    return newValues;
  }
  const diff = Date.now() - beganAt;
  if (diff < ms) {
    await sleep(ms - diff);
  }
  return batchPromises(promises.slice(q), q, ms, newValues);
}

function initTest() {
  const promiseList = Array(15).fill(undefined).map((_, i) => () => sleep(Math.random() * 1000, i));
  batchPromises(promiseList, 5, 250)
    .then(res => console.log('final', res), console.error)
}

// initTest();

module.exports = {
  sleep,
  batchPromises,
};
