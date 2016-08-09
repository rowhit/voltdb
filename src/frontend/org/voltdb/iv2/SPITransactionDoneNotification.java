/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.iv2;

/**
 * SPIs uses this to notify the SpScheduler when transaction finishes. The safe
 * notification point is when we mark the transaction done on the Site
 * thread. This is implemented by the SpScheduler and passed in as a parameter
 * to TransactionTask.
 */
public interface SPITransactionDoneNotification {
    /**
     * Notifies immediately when a transaction is done, either rolled back or
     * committed.
     * @param spHandle    The spHandle of the transaction. For MP transactions,
     *                    this is the spHandle of the FIRST fragment.
     */
    void transactionDone(long spHandle);
}
