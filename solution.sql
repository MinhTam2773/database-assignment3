SET SERVEROUTPUT ON;

DECLARE
BEGIN
   -- Outer loop: select distinct transaction numbers to process one transaction at a time
   FOR txn_id_rec IN (
      SELECT DISTINCT transaction_no
        FROM new_transactions
   ) LOOP
      BEGIN
         -- Check if transaction number is NULL, raise error if yes
         IF txn_id_rec.transaction_no IS NULL THEN
            RAISE_APPLICATION_ERROR(
               -20001,
               'Missing transaction number'
            );
         END IF;

         -- Inner loop: process all rows for the current transaction
         FOR txn_row_rec IN (
            SELECT transaction_no,
                   transaction_date,
                   description,
                   account_no,
                   transaction_type,
                   transaction_amount
              FROM new_transactions
             WHERE transaction_no = txn_id_rec.transaction_no
         ) LOOP
            DBMS_OUTPUT.PUT_LINE('Processing: transaction id: '
                                 || txn_row_rec.transaction_no
                                 || ' amount: '
                                 || txn_row_rec.transaction_amount
                                 || ' type: ' || txn_row_rec.transaction_type);
         END LOOP;

      EXCEPTION
         WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
            -- Insert error message into error log table
            INSERT INTO wkis_error_log (error_msg) VALUES (SQLERRM);
      END;
   END LOOP;
END;
/
