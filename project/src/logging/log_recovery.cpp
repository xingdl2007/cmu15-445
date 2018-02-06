/**
 * log_recovey.cpp
 */

#include "logging/log_recovery.h"
#include "page/table_page.h"

namespace cmudb {
/*
 * deserialize a log record from log buffer
 * @return: true means deserialize succeed, otherwise can't deserialize cause
 * incomplete log record
 */
bool LogRecovery::DeserializeLogRecord(const char *data,
                                             LogRecord &log_record) {
  return false;
}

/*
 *redo phase on TABLE PAGE level(table/table_page.h)
 *read log file from the beginning to end (you must prefetch log records into
 *log buffer to reduce unnecessary I/O operations), remember to compare page's
 *LSN with log_record's sequence number, and also build active_txn_ table &
 *lsn_mapping_ table
 */
void LogRecovery::Redo() {}

/*
 *undo phase on TABLE PAGE level(table/table_page.h)
 *iterate through active txn map and undo each operation
 */
void LogRecovery::Undo() {}

} // namespace cmudb
