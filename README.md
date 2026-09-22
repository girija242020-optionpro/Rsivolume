# Dhan Ultimate Backend v2.0

Universal Dhan data plane for the RSI + DEMA + Volume PWA.

## Important fix in v2
The Dhan compact master uses `SEM_EXM_EXCH_ID + SEM_SEGMENT`. For an NSE index the mapping is `NSE:I -> IDX_I`. The previous backend built `NSE_I`, so the PWA's `IDX_I` search could return no NIFTY instrument. v2 fixes that and also provides a direct index map: NIFTY=IDX_I/13, SENSEX=IDX_I/1.

## Render
Build: `npm install`
Start: `npm start`
Node: 20+

Set Dhan credentials and VAPID values as Render environment variables. Never put the Dhan secret or VAPID private key in the PWA.

## Verify
- `/health`
- `/api/v1/status`
- `/api/v1/bootstrap?symbol=NIFTY`
- `/api/v1/instruments/search?q=NIFTY&exchangeSegment=IDX_I&limit=10`

Expected NIFTY mapping: `exchangeSegment=IDX_I`, `securityId=13`.
