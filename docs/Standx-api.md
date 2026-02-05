# StandX API Documentation

> **Note:** This document was aggregated from the official StandX documentation. Some sections (HTTP API and WebSocket API) were reconstructed from available summaries as the original pages were inaccessible.

## Table of Contents

1.  [Overview](#overview)
2.  [Authentication](#authentication)
3.  [EVM Authentication Example](#evm-authentication-example)
4.  [Solana Authentication Example](#solana-authentication-example)
5.  [HTTP API](#http-api)
6.  [WebSocket API](#websocket-api)
7.  [API Reference](#api-reference)
8.  [API Token Management](#api-token-management)

---

## Overview

StandX provides REST and WebSocket APIs for perpetual futures trading. Programmatically access real-time market data, manage positions, execute trades, and monitor your portfolio.

### Base URLs

*   **HTTP API:** `https://perps.standx.com`
*   **WebSocket API:** `wss://perps.standx.com/ws-stream/v1`

### Getting Started

Start by obtaining your JWT token via wallet signature authentication.

### Tips

Building and operating automated trading systems requires careful consideration and experience. Factors such as network instability, latency, and unexpected edge cases must be accounted for. Please manage your system actively and be responsible for your own risk management.

*Last updated: January 15, 2026*

---

## Authentication

⚠️ This document is under construction.

This document explains how to obtain JWT access tokens for the StandX Perps API through wallet signatures.

### Prerequisites

*   Valid wallet address and corresponding private key
*   Development environment with `ed25519` algorithm support

### Authentication Flow

#### 1. Prepare Wallet and Temporary `ed25519` Key Pair

*   **Prepare Wallet:** Ensure you have a blockchain wallet with its address and private key.
*   **Generate Temporary `ed25519` Key Pair and `requestId`**

#### 2. Get Signature Data

Request signature data from the server:

*Note: Code examples provided below are for reference purposes only and demonstrate the general implementation approach. Adapt them to your specific production environment.*

**Using `curl`**

```bash
curl 'https://api.standx.com/v1/offchain/prepare-signin?chain=<chain>' \
  -H 'Content-Type: application/json' \
  --data-raw '{ "address": "<your_wallet_address>", "requestId": "<base58_encoded_public_key>" }'
```

**Request Parameters**

*   **`chain`** `string` **Yes** Blockchain network: `bsc` or `solana`
*   **`address`** `string` **Yes** Wallet address
*   **`requestId`** `string` **Yes** Base58-encoded `ed25519` public key from step 1

**Success Response**

```json
{
  "success": true,
  "signedData": "eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9..."
}
```

#### 3. Parse and Verify Signature Data

`signedData` is a JWT string that must be verified using StandX's public key.

**Get Verification Public Key**

```bash
curl 'https://api.standx.com/v1/offchain/certs'
```

#### 4. Sign the Message

Sign `payload.message` with your wallet private key to generate the `signature`.

*   **EVM (BSC):** Use `wallet.signMessage(payload.message)`.
*   **Solana:** Sign the message bytes with Ed25519. Requires a specific JSON format for the signature (see examples).

#### 5. Get Access Token

Submit the `signature` and original `signedData` to the login endpoint.

**Optional Parameter:**

*   `expiresSeconds` (`number`): Token expiration time in seconds. Defaults to `604800` (7 days) if not specified.

**Using `curl`**

```bash
curl 'https://api.standx.com/v1/offchain/login?chain=<chain>' \
  -H 'Content-Type: application/json' \
  --data-raw '{ "signature": "0x...", "signedData": "eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9...", "expiresSeconds": 604800 }'
```

**Success Response**

```json
{
  "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "address": "0x...",
  "alias": "user123",
  "chain": "bsc",
  "perpsAlpha": true
}
```

#### 6. Use Access Token

Use the obtained token for subsequent API requests by adding `Authorization: Bearer <token>` to the request headers.

### Body Signature Flow

Some endpoints may require a body signature.

1.  Prepare a key pair
2.  Build message: `{version},{id},{timestamp},{payload}`
3.  Sign with private key
4.  Base64 encode signature
5.  Attach signature to request headers:
    *   `x-request-sign-version`
    *   `x-request-id`
    *   `x-request-timestamp`
    *   `x-request-signature`

---

## EVM Authentication Example

This example demonstrates how to authenticate with the StandX Perps API using an EVM-compatible wallet (e.g., BSC).

### Prerequisites

*   Node.js environment with TypeScript support
*   EVM wallet with private key
*   Required packages: `npm install @noble/curves @scure/base ethers`

### Implementation

```typescript
import { ed25519 } from "@noble/curves/ed25519";
import { base58 } from "@scure/base";
import { ethers } from "ethers";

// ... (See full implementation in original source or perps-auth-evm-example page)

// Key Usage:
// 1. Initialize StandXAuth
// 2. Setup ethers wallet
// 3. Call auth.authenticate("bsc", wallet.address, (msg) => wallet.signMessage(msg))
```

**Environment Variables**

Create a `.env` file with `WALLET_PRIVATE_KEY=your_private_key_here`.

---

## Solana Authentication Example

This example demonstrates how to authenticate with the StandX Perps API using a Solana wallet.

### Prerequisites

*   Node.js environment with TypeScript support
*   Solana wallet with private key (base58-encoded)
*   Required packages: `npm install @noble/curves @scure/base @solana/web3.js bs58`

### Implementation

Solana authentication differs in the signature format. It requires a JSON structure containing the input payload and the output signature/account details, all base64 encoded.

```typescript
// ... (See full implementation in original source or perps-auth-svm-example page)

// Key Usage:
// Call auth.authenticate("solana", walletAddress, async (message, payload) => { ... })
// Inside callback: Construct the specific JSON format required for Solana signatures.
```

---

## HTTP API

### API Overview

**Base URL**: `https://perps.standx.com`

#### Authentication

All endpoints (except public ones) require JWT authentication. Include the JWT token in the `Authorization` header:

`Authorization: Bearer <your_jwt_token>`

Token Validity: 7 days

#### Body Signature

Some endpoints require a body signature. Add the following headers to signed requests:

*   `x-request-sign-version: v1`
*   `x-request-id: <random_string>`
*   `x-request-timestamp: <timestamp_in_milliseconds>`
*   `x-request-signature: <your_body_signature>`

See the [Authentication Guide](#authentication) for implementation details.

#### Session ID

For `new_order` and `cancel_order` requests, you need to know the result of these requests after actual matching. To get these results, you need to add the following information to the headers of these interface requests:

`x-session-id: <your_custom_session_id>`

Note that this `session_id` needs to be consistent with the `session_id` used in your ws-client.

#### Request Format

*   `int` parameters (e.g., `timestamp`) are expected as JSON integers, not strings.
*   `decimal` parameters (e.g., `price`) are expected as JSON strings, not floats.

### Trade Endpoints

#### Create New Order

`POST /api/new_order`

**Note**: A successful response indicates the order was submitted, not necessarily executed. Some orders (e.g., ALO) may be rejected during matching if conditions are not met. Subscribe to the Order Response Stream for real-time execution status.

To receive order updates via the Order Response Stream, add the `x-session-id` header to the request. This `session_id` must match the `session_id` used in your ws-client.

**Requires Authentication** • **Requires Body Signature**

**Required Parameters**

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `symbol` | `string` | Trading pair (see Reference) |
| `side` | `enum` | Order side (see Reference) |
| `order_type` | `enum` | Order type (see Reference) |
| `qty` | `decimal` | Order quantity |
| `time_in_force` | `enum` | Time in force (see Reference) |
| `reduce_only` | `boolean` | If true, only reduces position |

**Optional Parameters**

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `price` | `decimal` | Order price (required for limit orders) |
| `cl_ord_id` | `string` | Client order ID (auto-generated if omitted) |
| `margin_mode` | `enum` | Margin mode (see Reference). Must match position |
| `leverage` | `int` | Leverage value. Must match position |

**Request Example**:

```json
{
  "symbol": "BTC-USD",
  "side": "buy",
  "order_type": "limit",
  "qty": "0.1",
  "price": "50000",
  "time_in_force": "gtc",
  "reduce_only": false
}
```

**Response Example**:

```json
{
  "code": 0,
  "message": "success",
  "request_id": "xxx-xxx-xxx"
}
```

#### Cancel Order

`POST /api/cancel_order`

To receive order updates via the Order Response Stream, add the `x-session-id` header to the request. This `session_id` must match the `session_id` used in your ws-client.

**Requires Authentication** • **Requires Body Signature**

**Parameters**

At least one of `order_id` or `cl_ord_id` is required.

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `order_id` | `int` | Order ID to cancel |
| `cl_ord_id` | `string` | Client Order ID to cancel |

**Request Example**:

```json
{
  "order_id": 2424844
}
```

**Response Example**:

```json
{
  "code": 0,
  "message": "success",
  "request_id": "xxx-xxx-xxx"
}
```

#### Cancel Multiple Orders

`POST /api/cancel_orders`

**Requires Authentication** • **Requires Body Signature**

**Parameters**

At least one of `order_id_list` or `cl_ord_id_list` is required.

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `order_id_list` | `int[]` | List of Order IDs to cancel |
| `cl_ord_id_list` | `string[]` | List of Client Order IDs to cancel |

**Request Example**:

```json
{
  "order_id_list": [2424844]
}
```

**Response Example**:

```json
[]
```

#### Change Leverage

`POST /api/change_leverage`

**Requires Authentication** • **Requires Body Signature**

**Required Parameters**

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `symbol` | `string` | Trading pair (see Reference) |
| `leverage` | `int` | New leverage value |

**Request Example**:

```json
{
  "symbol": "BTC-USD",
  "leverage": 10
}
```

**Response Example**:

```json
{
  "code": 0,
  "message": "success",
  "request_id": "xxx-xxx-xxx"
}
```

#### Change Margin Mode

`POST /api/change_margin_mode`

**Requires Authentication** • **Requires Body Signature**

**Required Parameters**

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `symbol` | `string` | Trading pair (see Reference) |
| `margin_mode` | `enum` | Margin mode (see Reference) |

**Request Example**:

```json
{
  "symbol": "BTC-USD",
  "margin_mode": "cross"
}
```

**Response Example**:

```json
{
  "code": 0,
  "message": "success",
  "request_id": "xxx-xxx-xxx"
}
```

### User Endpoints

#### Transfer Margin

`POST /api/transfer_margin`

**Requires Authentication** • **Requires Body Signature**

**Required Parameters**

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `symbol` | `string` | Trading pair (see Reference) |
| `amount_in` | `decimal` | Amount to transfer |

**Request Example**:

```json
{
  "symbol": "BTC-USD",
  "amount_in": "1000.0"
}
```

**Response Example**:

```json
{
  "code": 0,
  "message": "success",
  "request_id": "xxx-xxx-xxx"
}
```

#### Query Order

`GET /api/query_order`

**⚠️ Note**: Orders may be rejected due to async matching. To receive real-time order updates, check the Order Response Stream.

**Requires Authentication**

**Query Parameters**

At least one of `order_id` or `cl_ord_id` is required.

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `order_id` | `int` | Order ID to query |
| `cl_ord_id` | `string` | Client Order ID to query |

**Response Example**:

```json
{
  "avail_locked": "3.071880000",
  "cl_ord_id": "01K2BK4ZKQE0C308SRD39P8N9Z",
  "closed_block": -1,
  "created_at": "2025-08-11T03:35:25.559151Z",
  "created_block": -1,
  "fill_avg_price": "0",
  "fill_qty": "0",
  "id": 1820682,
  "leverage": "10",
  "liq_id": 0,
  "margin": "0",
  "order_type": "limit",
  "payload": null,
  "position_id": 15,
  "price": "121900.00",
  "qty": "0.060",
  "reduce_only": false,
  "remark": "",
  "side": "sell",
  "source": "user",
  "status": "open",
  "symbol": "BTC-USD",
  "time_in_force": "gtc",
  "updated_at": "2025-08-11T03:35:25.559151Z",
  "user": "bsc_0x..."
}
```

#### Query User Orders

`GET /api/query_orders`

**Requires Authentication**

**Query Parameters**

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `symbol` | `string` | Trading pair (see Reference) |
| `status` | `enum` | Order status (see Reference) |
| `order_type` | `enum` | Order type (see Reference) |
| `start` | `string` | Start time (ISO 8601) |
| `end` | `string` | End time (ISO 8601) |
| `last_id` | `number` | Last order ID for pagination |
| `limit` | `number` | Result limit (default: 100, max: 500) |

**Response Example**:

```json
{
  "page_size": 1,
  "result": [
    {
      "avail_locked": "3.071880000",
      "cl_ord_id": "01K2BK4ZKQE0C308SRD39P8N9Z",
      "closed_block": -1,
      "created_at": "2025-08-11T03:35:25.559151Z",
      "created_block": -1,
      "fill_avg_price": "0",
      "fill_qty": "0",
      "id": 1820682,
      "leverage": "10",
      "liq_id": 0,
      "margin": "0",
      "order_type": "limit",
      "payload": null,
      "position_id": 15,
      "price": "121900.00",
      "qty": "0.060",
      "reduce_only": false,
      "remark": "",
      "side": "sell",
      "source": "user",
      "status": "new",
      "symbol": "BTC-USD",
      "time_in_force": "gtc",
      "updated_at": "2025-08-11T03:35:25.559151Z",
      "user": "bsc_0x..."
    }
  ],
  "total": 1
}
```

#### Query User All Open Orders

`GET /api/query_open_orders`

**Requires Authentication**

**Query Parameters**

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `symbol` | `string` | Trading pair (see Reference) |
| `limit` | `number` | Result limit (default: 500, max: 1200) |

**Response Example**:

```json
{
  "page_size": 1,
  "result": [
    {
      "avail_locked": "3.071880000",
      "cl_ord_id": "01K2BK4ZKQE0C308SRD39P8N9Z",
      "closed_block": -1,
      "created_at": "2025-08-11T03:35:25.559151Z",
      "created_block": -1,
      "fill_avg_price": "0",
      "fill_qty": "0",
      "id": 1820682,
      "leverage": "10",
      "liq_id": 0,
      "margin": "0",
      "order_type": "limit",
      "payload": null,
      "position_id": 15,
      "price": "121900.00",
      "qty": "0.060",
      "reduce_only": false,
      "remark": "",
      "side": "sell",
      "source": "user",
      "status": "new",
      "symbol": "BTC-USD",
      "time_in_force": "gtc",
      "updated_at": "2025-08-11T03:35:25.559151Z",
      "user": "bsc_0x..."
    }
  ],
  "total": 1
}
```

#### Query User Trades

`GET /api/query_trades`

**Requires Authentication**

**Query Parameters**

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `symbol` | `string` | Trading pair (see Reference) |
| `last_id` | `number` | Last trade ID for pagination |
| `side` | `string` | Order side (see Reference) |
| `start` | `string` | Start time (ISO 8601) |
| `end` | `string` | End time (ISO 8601) |
| `limit` | `number` | Result limit (default: 100, max: 500) |

**Response Example**:

```json
{
  "page_size": 1,
  "result": [
    {
      "created_at": "2025-08-11T03:36:19.352620Z",
      "fee_asset": "DUSD",
      "fee_qty": "0.121900",
      "id": 409870,
      "order_id": 1820682,
      "pnl": "1.62040",
      "price": "121900",
      "qty": "0.01",
      "side": "sell",
      "symbol": "BTC-USD",
      "updated_at": "2025-08-11T03:36:19.352620Z",
      "user": "bsc_0x...",
      "value": "1219.00"
    }
  ],
  "total": 1
}
```

#### Query Position Config

`GET /api/query_position_config`

**Requires Authentication**

**Required Parameters**

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `symbol` | `string` | Trading pair (see Reference) |

**Response Example**:

```json
{
  "symbol": "BTC-USD",
  "leverage": 10,
  "margin_mode": "cross"
}
```

#### Query User Positions

`GET /api/query_positions`

**Requires Authentication**

**Query Parameters**

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `symbol` | `string` | Trading pair (see Reference) |

**Response Example**:

```json
[
  {
    "bankruptcy_price": "109608.01",
    "created_at": "2025-08-10T09:05:50.265265Z",
    "entry_price": "121737.96",
    "entry_value": "114433.68240",
    "holding_margin": "11443.3682400",
    "id": 15,
    "initial_margin": "11443.36824",
    "leverage": "10",
    "liq_price": "112373.50",
    "maint_margin": "2860.30367500",
    "margin_asset": "DUSD",
    "margin_mode": "isolated",
    "mark_price": "121715.05",
    "mmr": "3.993223845366698695025800014",
    "position_value": "114412.14700",
    "qty": "0.940",
    "realized_pnl": "31.61532",
    "status": "open",
    "symbol": "BTC-USD",
    "time": "2025-08-11T03:41:40.922818Z",
    "updated_at": "2025-08-10T09:05:50.265265Z",
    "upnl": "-21.53540",
    "user": "bsc_0x..."
  }
]
```

#### Query User Balances

`GET /api/query_balance`

**Requires Authentication**

**Description**: Unified balance snapshot.

**Response Fields**:

| Name | Type | Description |
| :--- | :--- | :--- |
| `isolated_balance` | `decimal` | Total isolated wallet balance |
| `isolated_upnl` | `decimal` | Isolated unrealized PnL |
| `cross_balance` | `decimal` | Cross wallet available balance |
| `cross_margin` | `decimal` | Cross used margin (executed positions only) |
| `cross_upnl` | `decimal` | Cross unrealized PnL |
| `locked` | `decimal` | Order locked (margin + fee) |
| `cross_available` | `decimal` | `cross_balance - cross_margin - locked + cross_upnl` |
| `balance` | `decimal` | Total Asset = `cross_balance + isolated_balance` |
| `upnl` | `decimal` | Total Unrealized PnL = `cross_upnl + isolated_upnl` |
| `equity` | `decimal` | Account Equity = `balance + upnl` |
| `pnl_freeze` | `decimal` | 24h Realized PnL (for display) |

**Response Example**:

```json
{
  "isolated_balance": "11443.3682400",
  "isolated_upnl": "-21.53540",
  "cross_balance": "1088575.259316737",
  "cross_margin": "2860.30367500",
  "cross_upnl": "31.61532",
  "locked": "0.000000000",
  "cross_available": "1085746.571",
  "balance": "1100018.627556737",
  "upnl": "10.07992",
  "equity": "1100028.707476657",
  "pnl_freeze": "31.61532"
}
```

### Public Endpoints

#### Query Symbol Info

`GET /api/query_symbol_info`

**Required Parameters**

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `symbol` | `string` | Trading pair (see Reference) |

**Response Example**:

```json
[
  {
    "base_asset": "BTC",
    "base_decimals": 9,
    "created_at": "2025-07-10T05:15:32.089568Z",
    "def_leverage": "10",
    "depth_ticks": "0.01,0.1,1",
    "enabled": true,
    "maker_fee": "0.0001",
    "max_leverage": "20",
    "max_open_orders": "100",
    "max_order_qty": "100",
    "max_position_size": "1000",
    "min_order_qty": "0.001",
    "price_cap_ratio": "0.3",
    "price_floor_ratio": "0.3",
    "price_tick_decimals": 2,
    "qty_tick_decimals": 3,
    "quote_asset": "DUSD",
    "quote_decimals": 9,
    "symbol": "BTC-USD",
    "taker_fee": "0.0004",
    "updated_at": "2025-07-10T05:15:32.089568Z"
  }
]
```

#### Query Symbol Market

`GET /api/query_symbol_market`

**Required Parameters**

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `symbol` | `string` | Trading pair (see Reference) |

**Response Example**:

```json
{
  "base": "BTC",
  "funding_rate": "0.00010000",
  "high_price_24h": "122164.08",
  "index_price": "121601.158461",
  "last_price": "121599.94",
  "low_price_24h": "114098.44",
  "mark_price": "121602.43",
  "mid_price": "121599.99",
  "next_funding_time": "2025-08-11T08:00:00Z",
  "open_interest": "15.948",
  "quote": "DUSD",
  "spread": ["121599.94", "121600.04"],
  "symbol": "BTC-USD",
  "time": "2025-08-11T03:44:40.922233Z",
  "volume_24h": "9030.51800000000002509"
}
```

#### Query Symbol Price

`GET /api/query_symbol_price`

**Required Parameters**

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `symbol` | `string` | Trading pair (see Reference) |

**Response Example**:

```json
{
  "base": "BTC",
  "index_price": "121601.158461",
  "last_price": "121599.94",
  "mark_price": "121602.43",
  "mid_price": "121599.99",
  "quote": "DUSD",
  "spread_ask": "121600.04",
  "spread_bid": "121599.94",
  "symbol": "BTC-USD",
  "time": "2025-08-11T03:44:40.922233Z"
}
```

#### Query Depth Book

`GET /api/query_depth_book`

**⚠️ Note**: Order of price levels in `asks` and `bids` arrays is NOT guaranteed. Implement local sorting on client side based on your requirements.

**Required Parameters**

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `symbol` | `string` | Trading pair (see Reference) |

**Response Example**:

```json
{
  "asks": [
    ["121895.81", "0.843"],
    ["121896.11", "0.96"]
  ],
  "bids": [
    ["121884.01", "0.001"],
    ["121884.31", "0.001"]
  ],
  "symbol": "BTC-USD"
}
```

#### Query Recent Trades

`GET /api/query_recent_trades`

**Required Parameters**

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `symbol` | `string` | Trading pair (see Reference) |

**Response Example**:

```json
[
  {
    "is_buyer_taker": true,
    "price": "121720.18",
    "qty": "0.01",
    "quote_qty": "1217.2018",
    "symbol": "BTC-USD",
    "time": "2025-08-11T03:48:47.086505Z"
  },
  {
    "is_buyer_taker": true,
    "price": "121720.18",
    "qty": "0.01",
    "quote_qty": "1217.2018",
    "symbol": "BTC-USD",
    "time": "2025-08-11T03:48:46.850415Z"
  }
]
```

#### Query Funding Rates

`GET /api/query_funding_rates`

**Required Parameters**

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `symbol` | `string` | Trading pair (see Reference) |
| `start_time` | `int` | Start time (ms) |
| `end_time` | `int` | End time (ms) |

**Response Example**:

```json
[
  {
    "id": 1,
    "symbol": "BTC-USD",
    "funding_rate": "0.0001",
    "index_price": "121601.158461",
    "mark_price": "121602.43",
    "premium": "0.0001",
    "time": "2025-08-11T03:48:47.086505Z",
    "created_at": "2025-08-11T03:48:47.086505Z",
    "updated_at": "2025-08-11T03:48:47.086505Z"
  }
]
```

### Kline Endpoints

#### Get Server Time

`GET /api/kline/time`

**Response Example**:

```
1620000000
```

#### Get Kline History

`GET /api/kline/history`

**Required Parameters**

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `symbol` | `string` | Trading pair (see Reference) |
| `from` | `u64` | Unix timestamp (seconds) |
| `to` | `u64` | Unix timestamp (seconds) |
| `resolution` | `enum` | Resolution (see Reference) |

**Optional Parameters**

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `countback` | `u64` | Number of K-lines to load |

**Response Example**:

```json
{
  "s": "ok",
  "t": [1754897028, 1754897031],
  "c": [121897.95, 121903.04],
  "o": [121896.02, 121898.05],
  "h": [121897.95, 121903.15],
  "l": [121895.92, 121898.05],
  "v": [0.09, 10.542]
}
```

### Health Check

#### Health

`GET /api/health`

**Response**: `OK`

### Misc

#### Region and Server Time

`GET https://geo.standx.com/v1/region`

**Response Example**:

```json
{
  "systemTime": 1761970177865,
  "region": "jp"
}
```

## WebSocket API

WebSocket API provides two streams: **Market Stream** for market data/user updates and **Order Response Stream** for async order responses.

⚠️ This document is under construction.

### Connection Management

Both WebSocket streams implement the following connection management behavior:

#### Connection Duration Limit

*   **Max Duration**: 24 hours per connection.
*   Automatically terminated after 24 hours.
*   Clients should implement reconnection logic.

#### Ping/Pong Mechanism

*   **Server Ping Interval**: Every 10 seconds.
*   **Client Response**: Must respond with `Pong` frame.
*   **Timeout**: Connection terminated if no Pong within 5 minutes.

    ```json
    {
      "code": 408,
      "message": "disconnecting due to not receive Pong within 5 minute period"
    }
    ```

**Note**: Most modern browsers/libs handle this automatically. If not, you can manually send a ping frame.

### Market Stream

**Base Endpoint**: `wss://perps.standx.com/ws-stream/v1`

#### Available Channels

```json
[
  // public channels
  { "channel": "price", "symbol": "<symbol>" },
  { "channel": "depth_book", "symbol": "<symbol>" },
  { "channel": "public_trade", "symbol": "<symbol>" },
  // user-level authenticated channels
  { "channel": "order" },
  { "channel": "position" },
  { "channel": "balance" },
  { "channel": "trade" },
]
```

#### Subscribe to Depth Book

⚠️ **Note**: Order of price levels in `asks` and `bids` is not guaranteed. Sort locally on client.

**Request**:

```json
{
  "subscribe": {
    "channel": "depth_book",
    "symbol": "BTC-USD"
  }
}
```

**Response**:

```json
{
  "seq": 3,
  "channel": "depth_book",
  "symbol": "BTC-USD",
  "data": {
    "asks": [
      ["121896.02", "0.839"],
      ["121896.32", "1.051"]
    ],
    "bids": [
      ["121884.22", "0.001"],
      ["121884.52", "0.001"]
    ],
    "symbol": "BTC-USD"
  }
}
```

#### Subscribe to Symbol Price

**Request**:

```json
{
  "subscribe": {
    "channel": "price",
    "symbol": "BTC-USD"
  }
}
```

**Response**:

```json
{
  "seq": 13,
  "channel": "price",
  "symbol": "BTC-USD",
  "data": {
    "base": "BTC",
    "index_price": "121890.651250",
    "last_price": "121897.95",
    "mark_price": "121897.56",
    "mid_price": "121898.00",
    "quote": "DUSD",
    "spread": ["121897.95", "121898.05"],
    "symbol": "BTC-USD",
    "time": "2025-08-11T07:23:50.923602474Z"
  }
}
```

#### Authentication Request (Log in with JWT)

**Request**:

```json
{
  "auth": {
    "token": "<your_jwt_token>",
    "streams": [{ "channel": "order" }]
  }
}
```

`auth.streams` is optional, allowing subscription immediately after auth.

**Response**:

```json
{
  "seq": 1,
  "channel": "auth",
  "data": {
    "code": 200,
    "msg": "success"
  }
}
```

#### User Orders Subscription

**Request**:

```json
{
  "subscribe": {
    "channel": "order"
  }
}
```

**Response**:

```json
{
  "seq": 35,
  "channel": "order",
  "data": {
    "avail_locked": "0",
    "cl_ord_id": "01K2C9H93Y42RW8KD6RSVWVDVV",
    "closed_block": -1,
    "created_at": "2025-08-11T10:06:37.182464902Z",
    "created_block": -1,
    "fill_avg_price": "121245.21",
    "fill_qty": "1.000",
    "id": 2547027,
    "leverage": "15",
    "liq_id": 0,
    "margin": "8083.013333334",
    "order_type": "market",
    "payload": null,
    "position_id": 15,
    "price": "121245.20",
    "qty": "1.000",
    "reduce_only": false,
    "remark": "",
    "side": "buy",
    "source": "user",
    "status": "filled",
    "symbol": "BTC-USD",
    "time_in_force": "ioc",
    "updated_at": "2025-08-11T10:06:37.182465022Z",
    "user": "bsc_0x..."
  }
}
```

#### User Position Subscription

**Request**:

```json
{
  "subscribe": {
    "channel": "position"
  }
}
```

**Response**:

```json
{
  "seq": 36,
  "channel": "position",
  "data": {
    "created_at": "2025-08-10T09:05:50.265265Z",
    "entry_price": "121677.65",
    "entry_value": "2879988.1154631481396099405228",
    "id": 15,
    "initial_margin": "191999.219856667",
    "leverage": "15",
    "margin_asset": "DUSD",
    "margin_mode": "isolated",
    "qty": "23.669",
    "realized_pnl": "158.197103148",
    "status": "open",
    "symbol": "BTC-USD",
    "updated_at": "2025-08-10T09:05:50.265265Z",
    "user": "bsc_0x..."
  }
}
```

#### User Balance Subscription

**Request**:

```json
{
  "subscribe": {
    "channel": "balance"
  }
}
```

**Response**:

```json
{
  "seq": 37,
  "channel": "balance",
  "data": {
    "account_type": "perps",
    "created_at": "2025-08-09T09:36:54.504639Z",
    "free": "906946.976225666",
    "id": "bsc_0x...",
    "inbound": "0",
    "is_enabled": true,
    "kind": "user",
    "last_tx": "",
    "last_tx_updated_at": 0,
    "locked": "0.000000000",
    "occupied": "0",
    "outbound": "0",
    "ref_id": 0,
    "token": "DUSD",
    "total": "923207.752500717",
    "updated_at": "2025-08-09T09:36:54.504639Z",
    "version": 0,
    "wallet_id": "bsc_0x..."
  }
}
```

### Order Response Stream

This WebSocket channel provides real-time order status updates for the new order API. Since order creation is asynchronous, this channel notifies clients of order responses, including ALO order rejections.

**Base Endpoint**: `wss://perps.standx.com/ws-api/v1`

#### Request Structure

All WebSocket requests follow this structure:

```json
{
  "session_id": "<uuid>",
  "request_id": "<uuid>",
  "method": "<method>",
  "header": {
    "x-request-id": "",
    "x-request-timestamp": "",
    "x-request-signature": ""
  },
  "params": "<json string>"
}
```

**Fields**:

*   `session_id`: Consistent UUID throughout the session.
*   `request_id`: Unique UUID per request.
*   `method`: Action to perform (`auth:login`, `order:new`, `order:cancel`).
*   `header`: Required for `order:new` and `order:cancel` methods (auth headers).
*   `params`: JSON stringified parameters specific to the method.

#### Methods

**auth:login**

Authenticate with JWT token.

**Parameters**:

```json
{
  "token": "<jwt>"
}
```

**Request Example**:

```json
{
  "session_id": "consistent-session-id",
  "request_id": "unique-request-id",
  "method": "auth:login",
  "params": "{\"token\":\"your.jwt.token\"}"
}
```

**order:new**

Create new order. Parameters are same as HTTP API `new_order` payload.

**order:cancel**

Cancel existing order. Parameters are same as HTTP API `cancel_order` payload.

#### Order Response Format

**Success Response**:

```json
{
  "code": 0,
  "message": "success",
  "request_id": "bccc2b23-03dc-4c2b-912f-4315ebbbb7e0"
}
```

**Rejection Response**:

```json
{
  "code": 400,
  "message": "alo order rejected",
  "request_id": "1187e114-1914-4111-8da1-2aaaa86bb1b9"
}
```

---

## API Reference

### Enums

#### Symbol
*   `BTC-USD`

#### Margin Mode
*   `cross`
*   `isolated`

#### Token
*   `DUSD`

#### Order Side
*   `buy`
*   `sell`

#### Order Type
*   `limit`
*   `market`

#### Order Status
*   `open`
*   `canceled`
*   `filled`
*   `rejected`
*   `untriggered`

#### Time In Force
*   **`gtc`** Good Til Canceled
*   **`ioc`** Immediate Or Cancel
*   **`alo`** Add Liquidity Only

#### Resolution
*   `1T` (1 tick), `3S`, `1`, `5`, `15`, `60`, `1D`, `1W`, `1M`

### Error Responses

*   **`400`** Bad Request
*   **`401`** Unauthorized
*   **`403`** Forbidden
*   **`404`** Not Found
*   **`429`** Too Many Requests
*   **`500`** Internal Server Error

---

## API Token Management

StandX provides API access via a self-custodial key system for programmatic trading. All API tokens and signing keys are generated client-side and stored only by you—StandX does not store, access, or have any ability to recover your keys. This decentralized approach ensures only you control your trading activity. No one can execute trades or withdrawals on your behalf without your keys.

### Accessing API Token Management

To manage your API tokens:

1.  Click the profile icon in the top-right corner and select the Session Management option.
2.  Alternatively, if you are logged in, navigate to [https://standx.com/user/session](https://standx.com/user/session).

### Creating an API Token

When generating a new API token, you can customize the following options:

#### Remark

Give your token a descriptive name (e.g., "Trading Only", "Bot Trading") to help you identify its purpose later.

#### Request Signing Key

*   **Self-provided** – Use your own signing key for request authentication.
*   **Generate** – Generate a signing key locally in your browser.

⚠️ **Important:** Regardless of which option you choose, the signing key is generated and stored solely on your device. StandX never receives or stores your signing key. You must back up this key securely yourself—if lost, it cannot be recovered.

#### Permissions

Configure what the API token is allowed to do:

*   **Trading** – Allows the token to execute trades on your behalf.
*   **Withdraw** – Allows the token to initiate withdrawals (use with caution).

💡 **Tip:** Follow the principle of least privilege. Only enable permissions that are absolutely necessary for your use case. For a trading bot, consider enabling only "Trading" and disabling "Withdraw".

#### Expiration

Choose how long the token remains valid:

*   7 Days – Short-term usage
*   30 Days – Standard usage
*   90 Days – Extended usage
*   180 Days – Long-term usage

### Security Best Practices

⚠️ **Warning:** Your API keys are self-custodial. You are solely responsible for their security.

Because of StandX’s decentralized, non-custodial model, we cannot recover lost keys or reverse unauthorized transactions. Follow these best practices:

*   **Back up your keys securely** – Store your API tokens and signing keys in a safe place; they cannot be recovered if lost.
*   **Never share your API keys** – Do not share your API token or signing key with anyone.
*   **Never commit keys to version control** – Use environment variables or secure secret management.
*   **Use restrictive permissions** – Only enable the permissions you need.
*   **Rotate keys regularly** – Generate new tokens and revoke old ones periodically.
*   **Monitor your sessions** – Regularly review your active sessions and revoke any you don’t recognize.

### Revoking an API Token

If you suspect your API key has been compromised, or if you no longer need it:

1.  Go to [https://standx.com/user/session](https://standx.com/user/session).
2.  Locate the token in your session list.
3.  Click "Revoke" to invalidate the token immediately.

Revoking a token is immediate and irreversible. You will need to generate a new token if you require API access again.

*Last updated: January 14, 2026*