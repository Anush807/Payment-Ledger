// k6 load test: ramp to a sustained order rate and report saga throughput.
// Run with: k6 run load-test/orders.js
import http from 'k6/http';
import { check } from 'k6';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

const ORDER = __ENV.ORDER || 'http://localhost:8081/api/v1';
const WALLET = __ENV.WALLET || 'http://localhost:8082/api/v1';
const API_KEY = __ENV.API_KEY || 'dev-key';
const AMOUNT = 4990;

export const options = {
  scenarios: {
    orders: {
      executor: 'ramping-arrival-rate',
      startRate: 10,
      timeUnit: '1s',
      preAllocatedVUs: 50,
      maxVUs: 200,
      stages: [
        { target: 50, duration: '30s' },
        { target: 100, duration: '1m' },
        { target: 100, duration: '2m' },
      ],
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(99)<1000'],
  },
};

function headers(idempotent) {
  const h = { 'Content-Type': 'application/json', 'X-Api-Key': API_KEY };
  if (idempotent) {
    h['Idempotency-Key'] = uuidv4();
  }
  return h;
}

export function setup() {
  const wallet = http.post(`${WALLET}/wallets`,
    JSON.stringify({ ownerId: 'k6', currency: 'INR' }), { headers: headers(false) });
  const walletId = wallet.json('walletId');
  // fund far more than the test will spend, so declines are never the bottleneck
  http.post(`${WALLET}/wallets/${walletId}/top-ups`,
    JSON.stringify({ amountMinor: 100000000000 }), { headers: headers(true) });
  return { walletId };
}

export default function (data) {
  const res = http.post(`${ORDER}/orders`, JSON.stringify({
    walletId: data.walletId,
    merchantId: 'm-42',
    amountMinor: AMOUNT,
    currency: 'INR',
  }), { headers: headers(true) });
  check(res, { 'order accepted': (r) => r.status === 202 });
}
