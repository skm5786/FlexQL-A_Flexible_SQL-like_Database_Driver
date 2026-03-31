/**
 * expiration.h  —  Background Row Expiry Thread  (Lesson 9)
 *
 * A single background thread wakes every EXPIRY_INTERVAL_SECONDS seconds,
 * walks every table in every database, acquires a WRITE lock, physically
 * removes expired rows from the linked list and hash index, and updates
 * row_count and the tail pointer.
 *
 * After each sweep, scans are faster because dead rows are gone.
 */
#ifndef FLEXQL_EXPIRATION_H
#define FLEXQL_EXPIRATION_H

#include "storage/dbmanager.h"

#define EXPIRY_INTERVAL_SECONDS 30

void expiry_start(DatabaseManager *mgr);
void expiry_stop(void);

#endif