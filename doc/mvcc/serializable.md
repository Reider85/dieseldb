# MVCC SERIALIZABLE SSI Implementation

## Overview

This document describes the MVCC SERIALIZABLE SSI (Serializable Snapshot Isolation) conflict detection implementation completed in prompt4.md #5.

## Design Decisions

### SSI Scope
- **Full SSI**: Both write-side check and read-set tracking implemented
- **Write-side check**: Detects ww-conflicts during UPDATE/DELETE operations
- **Read-set tracking**: Records rows read by SERIALIZABLE transactions for rw-conflict detection at commit time

### Victim Policy
- **Writer detection loser**: The transaction that commits/writes last (typically higher txid) gets aborted when a conflict is detected
- This ensures serializable execution by aborting the committing transaction when conflicts are found

### Exception Design
- `SerializationFailureException` extends `TransactionException` for backward compatibility with existing tests
- Existing tests that catch `TransactionException` for transaction-related failures will continue to work

## Implementation Details

### Core Components

#### 1. SerializationFailureException
- New exception class extending `TransactionException`
- Thrown when SERIALIZABLE transactions violate the serializable isolation contract
- Used for both write-write conflicts and read-write conflicts

#### 2. ConflictDetector
- Per-Database instance with cleanup on commit/rollback for heap stability
- Tracks active SERIALIZABLE transactions and their read/write sets
- Implements SSI conflict detection logic

#### 3. Database Integration
- Added `ConflictDetector` field to `Database` class
- Added SSI lifecycle hooks: `beginTracking`, `noteCommit`, `noteRollback`
- Added getter method for testing: `getConflictDetector()`

#### 4. Table Integration
- Added `checkSerializableWriteConflict` method to `Table` class
- Uses `SerializationFailureException` instead of `TransactionException` for SERIALIZABLE isolation

#### 5. Query Integration
- **UpdateQuery**: Uses SERIALIZABLE conflict check and tracks reads/writes
- **DeleteQuery**: Uses SERIALIZABLE conflict check and tracks reads/writes
- **SelectQuery**: Tracks reads for SERIALIZABLE transactions

#### 6. TupleVisibility Enhancement
- Added `hasSerializableWriteConflict` helper method for SSI conflict detection
- Encapsulates the same logic as `Table.checkWriteWriteConflict` as a standalone predicate

### Conflict Detection Logic

#### Write-Write Conflicts (ww-conflicts)
Detected during UPDATE/DELETE operations:
1. **Pending foreign change**: Another transaction has an uncommitted change on the target row
2. **Stale snapshot**: The row was committed by another transaction after the writer's snapshot

#### Read-Write Conflicts (rw-conflicts)
Detected at commit time for SERIALIZABLE transactions:
- Occurs when a transaction writes to a row that was read by an active SERIALIZABLE transaction whose snapshot predates the commit
- Victim policy: The committing transaction (writer) loses

### Transaction Lifecycle

1. **BEGIN**: Start tracking SERIALIZABLE transactions in the conflict detector
2. **READ/UPDATE/DELETE**: Track reads and writes for SERIALIZABLE transactions
3. **COMMIT**: Check for rw-conflicts before committing
4. **ROLLBACK**: Cleanup tracking state

## Testing

### New Test Class
- `SerializationConflictTest`: Unit tests for SSI conflict detection
- Tests conflict detection logic, transaction tracking, and exception handling

### Test Coverage
- Basic conflict detector functionality
- Write-write conflict detection
- Stale snapshot detection
- Read-write conflict detection at commit
- TupleVisibility helper method testing

## Integration Points

### Database Class
- Added `ConflictDetector conflictDetector` field
- Added `getConflictDetector()` getter method
- Modified `executeBeginTransaction()` to start tracking for SERIALIZABLE transactions
- Modified `executeCommit()` to check rw-conflicts and cleanup tracking
- Modified `executeRollback()` to cleanup tracking

### Table Class
- Added `checkSerializableWriteConflict()` method
- Modified `isRowVisibleToReader()` to track reads for SERIALIZABLE transactions

### Query Classes
- **UpdateQuery**: Uses SERIALIZABLE conflict check and tracks reads/writes
- **DeleteQuery**: Uses SERIALIZABLE conflict check and tracks reads/writes
- **SelectQuery**: Tracks reads for SERIALIZABLE transactions

### TupleVisibility Class
- Added `hasSerializableWriteConflict()` helper method

## Performance Considerations

### Memory Usage
- ConflictDetector tracks read/write sets for active SERIALIZABLE transactions
- Cleanup on commit/rollback prevents memory leaks
- ChainOf1000TxTest validates heap stability

### Performance Impact
- Read tracking adds minimal overhead to SELECT operations
- Write-side checks add minimal overhead to UPDATE/DELETE operations
- Commit-time rw-conflict detection only affects SERIALIZABLE transactions

## Backward Compatibility

- `SerializationFailureException` extends `TransactionException`
- Existing tests continue to work without modification
- No changes to existing isolation level behavior for non-SERIALIZABLE transactions

## Future Enhancements

### Potential Improvements
1. **Optimized conflict detection**: More efficient algorithms for large transaction sets
2. **Conflict resolution strategies**: Alternative victim policies
3. **Performance monitoring**: Metrics for conflict detection performance
4. **Advanced conflict reporting**: More detailed conflict information for debugging

### Integration Opportunities
1. **Deadlock detection**: Could be integrated with existing deadlock detection
2. **Performance optimization**: Query optimization could avoid conflicts where possible
3. **Monitoring**: Conflict detection metrics for database monitoring

## Validation

### Test Gates
- **Fast gate**: `make test-incr` (compile + fast test suite)
- **Acceptance gate**: `make timing` (full acceptance test suite)
- **Profile check**: Skipped (no JOIN/performance keywords in task description)

### Test Results
- All existing tests continue to pass
- New SSI tests validate conflict detection logic
- ChainOf1000TxTest validates heap stability during long transaction chains