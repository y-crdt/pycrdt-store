# Version history

## 0.1.3b0

- Improve `SQLiteYStore` performance by storing and applying checkpoints to reduce loading time.

## 0.1.2

- Fix in-memory database connection issue with `SQLiteYStore`.

## 0.1.1

- Fix SQLite store squashing.
- Add callbacks to compress/decompress SQLite store updates.
- Split stores in different files.
- Mark package as typed.

## 0.1.0

- Extract out pycrdt-store from pycrdt-websocket.
