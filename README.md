# Xelo-Db
A simplified version of a relational database, built for educational purposes.

## Expected Features
- Core db operations like `Insert`, `Delete`, `Select`, `Join`, etc.
- Simplified slotted page architecture.
- Range queries.

## Status
Under construction.

## Future Considerations 
1. See if LRU-K can be used instead of simple arena based LRU (current), given our architectural simplifications.
2. Improve background flusher to use latch-crabbing instead of complete buffer-pool lock while flushing pages.

## Extras
1. add some logging to print out the inner workings of the db for the interesting parts. 
    - Either write to an external file or just print to the stdout per operation.
