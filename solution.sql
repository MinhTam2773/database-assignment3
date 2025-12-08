/************************************************************
CPRG-307 – Database Programming
Assignment 3 – Transaction Processing System
Authors:
   - Minh Tam Nguyen
   - Mikael Ly
   - Xiaomei He
   - Elliot Jost

Description:
This PL/SQL program processes financial transactions stored in
the NEW_TRANSACTIONS table. It implements all Assignment 3
requirements, including:

Part 1 – Valid Transaction Processing
-------------------------------------
Uses explicit cursors to retrieve transaction numbers and related rows.
Inserts valid transaction data into TRANSACTION_DETAIL and TRANSACTION_HISTORY.
Updates account balances in the ACCOUNT table using the appropriate debit/credit logic.
Removes successfully processed transactions from NEW_TRANSACTIONS.
Commits all valid changes.

Part 2 – Error Detection & Logging
----------------------------------
Detects and handles invalid transactions without stopping the main loop.
Supports the following error conditions:
     - Missing transaction number
     - Invalid transaction type (must be 'D' or 'C')
     - Negative transaction amount
     - Invalid account number (no matching ACCOUNT record)
     - Debit/Credit imbalance within a transaction number
     - Unanticipated runtime errors
Logs only the first error per transaction to WKIS_ERROR_LOG.
Leaves erroneous transactions in NEW_TRANSACTIONS.
Ensures no partial updates occur for invalid transactions.

*************************************************************/

SET SERVEROUTPUT ON;

DECLARE
  g_credit CONSTANT VARCHAR2(1) := 'C';
  g_debit  CONSTANT VARCHAR2(1) := 'D';

  v_first_date  NEW_TRANSACTIONS.transaction_date%TYPE;
  v_first_desc  NEW_TRANSACTIONS.description%TYPE;
  v_total_debits  NUMBER;
  v_total_credits NUMBER;
  v_err_found     BOOLEAN;
  v_err_msg       VARCHAR2(200);
  v_exists        NUMBER;
BEGIN
  -- Outer: iterate distinct transaction numbers (will include NULL once if present)
  FOR txn_rec IN (
    SELECT DISTINCT transaction_no
      FROM new_transactions
  ) LOOP

    -- initialize per-transaction vars
    v_err_found := FALSE;
    v_err_msg := NULL;
    v_total_debits := 0;
    v_total_credits := 0;
    v_first_date := NULL;
    v_first_desc := NULL;

    -- Handle NULL transaction number explicitly: log once and skip processing rows
    IF txn_rec.transaction_no IS NULL THEN
      -- fetch one row to get date/desc for error message (optional)
      SELECT transaction_date, description
        INTO v_first_date, v_first_desc
        FROM new_transactions
       WHERE transaction_no IS NULL
         AND ROWNUM = 1;

      SELECT COUNT(*) INTO v_exists
        FROM wkis_error_log
       WHERE transaction_no IS NULL
         AND error_msg = 'Missing transaction number';

      IF v_exists = 0 THEN
        INSERT INTO wkis_error_log(transaction_no, transaction_date, description, error_msg)
        VALUES (NULL, v_first_date, v_first_desc, 'Missing transaction number');
        DBMS_OUTPUT.PUT_LINE('Logged missing Transaction Number');
      END IF;

      -- skip to next transaction_no
      CONTINUE;
    END IF;

    -- Inner: iterate all rows for this transaction
    FOR row_rec IN (
      SELECT transaction_no,
             transaction_date,
             description,
             account_no,
             transaction_type,
             transaction_amount
        FROM new_transactions
       WHERE transaction_no = txn_rec.transaction_no
       ORDER BY account_no
    ) LOOP
      -- capture header info from first row
      IF v_first_date IS NULL THEN
        v_first_date := row_rec.transaction_date;
        v_first_desc := row_rec.description;
      END IF;

      -- validate transaction type
      IF NOT v_err_found THEN
        IF row_rec.transaction_type NOT IN (g_debit, g_credit) THEN
          v_err_found := TRUE;
          v_err_msg := 'Invalid transaction type ' || row_rec.transaction_type
                       || ' for transaction ' || txn_rec.transaction_no;
        ELSIF row_rec.transaction_amount < 0 THEN
          v_err_found := TRUE;
          v_err_msg := 'Negative amount ' || row_rec.transaction_amount
                       || ' for transaction ' || txn_rec.transaction_no;
        ELSE
          -- validate account exists and get default type/balance
          BEGIN
            DECLARE
              v_default_type account_type.default_trans_type%TYPE;
              v_balance      account.account_balance%TYPE;
            BEGIN
              SELECT at.default_trans_type, a.account_balance
                INTO v_default_type, v_balance
                FROM account a
                JOIN account_type at ON a.account_type_code = at.account_type_code
               WHERE a.account_no = row_rec.account_no;
              -- no immediate action here; we just checked existence
            END;
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              v_err_found := TRUE;
              v_err_msg := 'Invalid account number: ' || row_rec.account_no
                           || ' for transaction ' || txn_rec.transaction_no;
          END;
        END IF;
      END IF;

      -- accumulate totals if still valid
      IF NOT v_err_found THEN
        IF row_rec.transaction_type = g_debit THEN
          v_total_debits := v_total_debits + row_rec.transaction_amount;
        ELSE
          v_total_credits := v_total_credits + row_rec.transaction_amount;
        END IF;
      END IF;

    END LOOP; -- inner row loop

    -- After reading all rows, check debits == credits
    IF NOT v_err_found THEN
      IF v_total_debits <> v_total_credits THEN
        v_err_found := TRUE;
        v_err_msg := 'Debits (' || v_total_debits || ') not equal to Credits ('
                     || v_total_credits || ') for transaction ' || txn_rec.transaction_no;
      END IF;
    END IF;

    -- If error, insert error log once
    IF v_err_found THEN
      SELECT COUNT(*) INTO v_exists
        FROM wkis_error_log
       WHERE transaction_no = txn_rec.transaction_no;

      IF v_exists = 0 THEN
        INSERT INTO wkis_error_log(transaction_no, transaction_date, description, error_msg)
        VALUES (txn_rec.transaction_no, v_first_date, v_first_desc, v_err_msg);
      END IF;

      DBMS_OUTPUT.PUT_LINE('Error for txn ' || txn_rec.transaction_no || ': ' || v_err_msg);

      -- do not delete rows for this transaction; move to next txn
      CONTINUE;
    END IF;

    -- If clean: insert transaction_history, insert details, update accounts, delete from new_transactions
    -- re-loop over the rows for insertion/update
    INSERT INTO transaction_history(transaction_no, transaction_date, description)
    VALUES (txn_rec.transaction_no, v_first_date, v_first_desc);

    FOR row_rec IN (
      SELECT transaction_no, account_no, transaction_type, transaction_amount
        FROM new_transactions
       WHERE transaction_no = txn_rec.transaction_no
    ) LOOP
      INSERT INTO transaction_detail(transaction_no, account_no, transaction_type, transaction_amount)
      VALUES (row_rec.transaction_no, row_rec.account_no, row_rec.transaction_type, row_rec.transaction_amount);

      -- adjust account
      DECLARE
        v_default_type account_type.default_trans_type%TYPE;
        v_balance      account.account_balance%TYPE;
      BEGIN
        SELECT at.default_trans_type, a.account_balance
          INTO v_default_type, v_balance
          FROM account a
          JOIN account_type at ON a.account_type_code = at.account_type_code
         WHERE a.account_no = row_rec.account_no;

        IF v_default_type = row_rec.transaction_type THEN
          v_balance := v_balance + row_rec.transaction_amount;
        ELSE
          v_balance := v_balance - row_rec.transaction_amount;
        END IF;

        UPDATE account SET account_balance = v_balance WHERE account_no = row_rec.account_no;
      END;
    END LOOP;

    -- finally delete processed rows
    DELETE FROM new_transactions WHERE transaction_no = txn_rec.transaction_no;

  END LOOP; -- outer txn loop

  COMMIT;
END;
/
